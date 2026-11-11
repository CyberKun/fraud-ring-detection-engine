#include "storage/wal.hpp"
#include <zlib.h>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace fraud::storage {
namespace {
namespace fs=std::filesystem;
[[noreturn]] void io(const char* op) { throw Error(std::string(op)+": "+std::strerror(errno)); }
struct File {
    int fd;
    explicit File(const fs::path& path,int flags) : fd(::open(path.c_str(),flags|O_CLOEXEC,0600)) {
        if (fd<0) io("open");
    }
    ~File(){ ::close(fd); }
};
void write_all(int fd,std::span<const std::uint8_t> data) {
    while (!data.empty()) {
        auto n=::write(fd,data.data(),data.size());
        if (n<0 && errno==EINTR) continue;
        if (n<=0) io("write");
        data=data.subspan(n);
    }
}
void read_at(int fd,off_t pos,std::span<std::uint8_t> out) {
    while (!out.empty()) {
        auto n=::pread(fd,out.data(),out.size(),pos);
        if (n<0 && errno==EINTR) continue;
        if (n<0) io("pread");
        if (!n) throw Error("unexpected file size change");
        pos+=n;
        out=out.subspan(n);
    }
}
off_t length(int fd) {
    struct stat st{};
    if (::fstat(fd,&st)) io("fstat");
    return st.st_size;
}
void sync(int fd) { if (::fsync(fd)) io("fsync"); }
void put(Bytes& bytes,Id n,unsigned width) {
    for (unsigned i=0;i<width;++i) bytes.push_back(static_cast<std::uint8_t>(n>>(8*i)));
}
struct BadFrame : Error { using Error::Error; };
struct TornFrame : BadFrame { using BadFrame::BadFrame; };
struct Reader {
    std::span<const std::uint8_t> data;
    Id get(unsigned width) {
        if (data.size()<width) throw BadFrame("short record");
        Id x=0;
        for (unsigned i=0;i<width;++i) x|=Id{data[i]}<<(8*i);
        data=data.subspan(width);
        return x;
    }
    bool boolean() {
        Id x=get(1);
        if (x>1) throw BadFrame("invalid boolean");
        return x!=0;
    }
    Key key() { return {get(8),get(8),static_cast<std::uint32_t>(get(4))}; }
};
Payload payload(Reader& r,Id tag) {
    switch(tag) {
    case 1: return EdgeUpsert{r.key(),r.get(8)};
    case 2: return EdgeDelete{r.key()};
    case 3: return VertexUpdate{r.get(8),{r.get(8),r.boolean()}};
    case 4: return Tick{};
    case 5: return Expire{r.get(8),r.get(8)};
    case 6: return HubSuppress{r.get(8)};
    case 7: return HubRestore{r.get(8)};
    default: throw BadFrame("unknown event tag");
    }
}
struct Header { std::size_t size,count; Id epoch,first; };
constexpr std::array<std::uint8_t,4> magic{'F','R','E','1'};
Header parse_header(std::span<const std::uint8_t> bytes,Id available) {
    if (bytes.size()<32) throw TornFrame("short header");
    Reader r{bytes};
    if (r.get(4)!=0x31455246) throw TornFrame("invalid FRE1 magic");
    if (r.get(2)!=1 || r.get(2)!=0) throw BadFrame("unsupported FRE1 version or flags");
    Header h{r.get(4),r.get(4),r.get(8),r.get(8)};
    if (!h.count || h.count>max_records || h.size>max_payload || h.count>h.size/22 ||
        !h.first || !h.epoch || h.count-1>UINT64_MAX-h.first) throw BadFrame("invalid FRE1 limits");
    if (available-32<h.size+4) throw TornFrame("short payload or checksum");
    return h;
}
Header header(int fd,off_t pos,off_t end) {
    if(end-pos<32) throw TornFrame("short header");
    std::array<std::uint8_t,32> bytes;
    read_at(fd,pos,bytes);
    return parse_header(bytes,end-pos);
}
Frame contents(std::span<const std::uint8_t> all,Header h) {
    Reader crc{all.last(4)};
    if (checksum(all.first(32+h.size))!=crc.get(4)) throw TornFrame("bad WAL CRC32");
    Reader r{all.subspan(32,h.size)};
    Frame frame;
    frame.reserve(h.count);
    for (std::size_t i=0;i<h.count;++i) {
        auto n=r.get(4);
        if (n>r.data.size()) throw BadFrame("invalid record length");
        Reader record{r.data.first(n)};
        r.data=r.data.subspan(n);
        Event e{record.get(8),h.epoch,h.first+i,record.get(8),{},Tick{}};
        auto tag=record.get(1);
        if (record.boolean()) e.source=ProducerSequence{record.get(8),record.get(8)};
        e.payload=payload(record,tag);
        if (!record.data.empty()) throw BadFrame("trailing record bytes");
        frame.push_back(e);
    }
    if (!r.data.empty()) throw BadFrame("trailing payload bytes");
    return frame;
}
Frame decode(int fd,off_t pos,Header h) {
    Bytes bytes(36+h.size);
    read_at(fd,pos,bytes);
    return contents(bytes,h);
}
// A damaged length can swallow the following frame. Look for a fully
// decodable frame after the damage before classifying it as a torn tail.
bool valid_suffix(int fd,off_t pos,off_t end) {
    std::array<std::uint8_t,65536> block;
    while (end-pos>=32) {
        auto n=static_cast<std::size_t>(std::min<off_t>(block.size(),end-pos));
        read_at(fd,pos,std::span(block).first(n));
        for (std::size_t i=0;i+4<=n;++i) {
            if (!std::equal(magic.begin(),magic.end(),block.begin()+i)) continue;
            try { decode(fd,pos+i,header(fd,pos+i,end)); return true; }
            catch (const BadFrame&) {}
        }
        pos+=n-3;
    }
    return false;
}
}
std::uint32_t checksum(std::span<const std::uint8_t> bytes) {
    return ::crc32_z(0,bytes.data(),bytes.size());
}
Frame decode_frame(std::span<const std::uint8_t> bytes) {
    auto h=parse_header(bytes,bytes.size());
    if(bytes.size()!=36+h.size) throw Error("replication frame length");
    auto frame=contents(bytes,h);
    for(std::size_t i=1;i<frame.size();++i)
        if(frame[i].time<frame[i-1].time) throw Error("replication frame time decreased");
    return frame;
}
void Wal::truncate(std::uint64_t bytes) {
    if(bytes>static_cast<Id>(length(fd_))) throw Error("cannot extend WAL by truncation");
    if(::ftruncate(fd_,bytes)) io("truncate");
    sync(fd_);
}
Bytes encode(const Frame& frame) {
    if (frame.empty() || frame.size()>max_records) throw Error("WAL record count cap");
    Bytes body;
    for (std::size_t i=0;i<frame.size();++i) {
        const auto& e=frame[i];
        if (!frame[0].lsn || !e.epoch || i>UINT64_MAX-frame[0].lsn || e.lsn!=frame[0].lsn+i ||
            e.epoch!=frame[0].epoch || (i && e.time<frame[i-1].time)) throw Error("invalid frame position");
        auto start=body.size();
        put(body,0,4);
        put(body,e.tenant,8); put(body,e.time,8);
        put(body,e.payload.index()+1,1); put(body,e.source.has_value(),1);
        if (e.source) { put(body,e.source->producer,8); put(body,e.source->sequence,8); }
        std::visit([&](auto x) {
            if constexpr (requires { x.key; }) {
                put(body,x.key.u,8); put(body,x.key.v,8); put(body,x.key.type,4);
            }
            if constexpr (requires { x.expiry; }) put(body,x.expiry,8);
            if constexpr (requires { x.vertex; }) put(body,x.vertex,8);
            if constexpr (requires { x.weight; }) { put(body,x.weight.volume,8); put(body,x.weight.flagged,1); }
            if constexpr (requires { x.handle; }) { put(body,x.handle,8); put(body,x.version,8); }
        },e.payload);
        auto len=body.size()-start-4;
        for (unsigned j=0;j<4;++j) body[start+j]=static_cast<std::uint8_t>(len>>(8*j));
    }
    if (body.size()>max_payload) throw Error("WAL payload cap");
    Bytes out(magic.begin(),magic.end());
    put(out,1,2); put(out,0,2); put(out,body.size(),4); put(out,frame.size(),4);
    put(out,frame[0].epoch,8); put(out,frame[0].lsn,8);
    out.insert(out.end(),body.begin(),body.end());
    put(out,checksum(out),4);
    return out;
}
Recovery recover(const fs::path& path,Recovery state,bool repair,const Sink& sink) {
    File file(path,repair ? O_RDWR : O_RDONLY);
    off_t end=length(file.fd);
    for (off_t pos=0;pos<end;) {
        Frame frame;
        Header h{};
        try {
            h=header(file.fd,pos,end);
            frame=decode(file.fd,pos,h);
        } catch (const TornFrame& error) {
            if (!repair || valid_suffix(file.fd,pos+1,end))
                throw Error("WAL corruption at offset "+std::to_string(pos)+": "+error.what());
            if (::ftruncate(file.fd,pos)) io("truncate");
            sync(file.fd);
            state.truncated+=end-pos;
            break;
        }
        if (h.first!=state.lsn+1 || h.epoch<state.epoch) throw Error("WAL position discontinuity");
        for (const auto& e:frame) {
            if (e.time<state.time) throw Error("WAL time decreased");
            state.time=e.time;
        }
        sink(frame);
        state.lsn=frame.back().lsn; state.epoch=h.epoch; ++state.frames;
        pos+=36+h.size;
    }
    return state;
}
void sync_directory(const fs::path& dir) { File f(dir,O_DIRECTORY|O_RDONLY); sync(f.fd); }
void atomic_file(const fs::path& path,std::span<const std::uint8_t> bytes) {
    auto tmp=path;
    tmp+=".tmp";
    File f(tmp,O_CREAT|O_TRUNC|O_WRONLY);
    write_all(f.fd,bytes);
    sync(f.fd);
    fs::rename(tmp,path);
    sync_directory(path.parent_path());
}
Bytes read_file(const fs::path& path,std::size_t cap) {
    File f(path,O_RDONLY);
    auto size=length(f.fd);
    if (static_cast<Id>(size)>cap) throw Error("file exceeds size cap");
    Bytes bytes(size);
    read_at(f.fd,0,bytes);
    return bytes;
}
Wal::Wal(const fs::path& path) : fd_(::open(path.c_str(),O_CREAT|O_APPEND|O_WRONLY|O_CLOEXEC,0600)) {
    if (fd_<0) io("open WAL");
    try { sync_directory(path.parent_path()); } catch (...) { ::close(fd_); throw; }
}
Wal::~Wal(){ ::close(fd_); }
WriteStats Wal::append(std::span<const Frame> frames) {
    std::size_t bytes=0;
    for (const auto& frame:frames) {
        auto data=encode(frame);
        write_all(fd_,data);
        bytes+=data.size();
    }
    auto start=std::chrono::steady_clock::now();
    sync(fd_);
    return {bytes,std::chrono::duration<double>(std::chrono::steady_clock::now()-start).count()};
}
} // namespace fraud::storage
