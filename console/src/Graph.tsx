import { useEffect, useRef } from 'react';
import {
  forceSimulation,
  forceManyBody,
  forceCenter,
  forceLink,
  forceCollide,
  SimulationNodeDatum,
  SimulationLinkDatum,
} from 'd3-force';
import { Focus } from 'lucide-react';
import type { Graph, Vertex } from './api';
type Node = Vertex & SimulationNodeDatum & { radius: number };
const colors = { ACCOUNT: '#4F8CFF', CARD: '#A78BFA', DEVICE: '#2DD4BF' };
export function Network({ graph, focus }: { graph: Graph; focus: string }) {
  const canvas = useRef<HTMLCanvasElement>(null),
    reset = useRef(() => {});
  useEffect(() => {
    const el = canvas.current!,
      ctx = el.getContext('2d')!;
    const nodes: Node[] = graph.vertices.map((v) => ({
      ...v,
      radius: 5 + Math.min(11, Math.log1p(Number(v.volume)) * 0.85),
    }));
    const links: SimulationLinkDatum<Node>[] = graph.edges.map((e) => ({
      source: e.u,
      target: e.v,
    }));
    const sim = forceSimulation(nodes)
      .force('charge', forceManyBody().strength(-210).theta(1.1))
      .force(
        'link',
        forceLink<Node, SimulationLinkDatum<Node>>(links)
          .id((n) => n.vertex_id)
          .distance(73)
          .iterations(1),
      )
      .force('center', forceCenter(0, 0))
      .force(
        'collision',
        forceCollide<Node>((n) => n.radius + 5),
      )
      .alphaDecay(0.06);
    let w = 1,
      h = 1,
      k = 1,
      px = 0,
      py = 0,
      auto = true,
      drag = false,
      x = 0,
      y = 0;
    function draw() {
      if (auto && nodes.length) {
        let extentX = 75,
          extentY = 65;
        for (const n of nodes) {
          extentX = Math.max(extentX, Math.abs(n.x!) + n.radius + 18);
          extentY = Math.max(extentY, Math.abs(n.y!) + n.radius + 18);
        }
        k = Math.min(1.45, w / (2 * extentX), h / (2 * extentY));
      }
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, w, h);
      ctx.translate(w / 2 + px, h / 2 + py);
      ctx.scale(k, k);
      ctx.beginPath();
      ctx.strokeStyle = '#42526b';
      ctx.lineWidth = 0.9 / k;
      for (const l of links) {
        const a = l.source as Node,
          b = l.target as Node;
        ctx.moveTo(a.x!, a.y!);
        ctx.lineTo(b.x!, b.y!);
      }
      ctx.stroke();
      for (const n of nodes) {
        const x = n.x!,
          y = n.y!,
          r = n.radius;
        ctx.fillStyle = colors[n.entity_type];
        ctx.beginPath();
        if (n.entity_type === 'ACCOUNT') ctx.arc(x, y, r, 0, 2 * Math.PI);
        else if (n.entity_type === 'CARD') ctx.rect(x - r, y - r, r * 2, r * 2);
        else {
          ctx.moveTo(x, y - r * 1.3);
          ctx.lineTo(x + r * 1.3, y);
          ctx.lineTo(x, y + r * 1.3);
          ctx.lineTo(x - r * 1.3, y);
          ctx.closePath();
        }
        ctx.fill();
        if (n.is_flagged && n.entity_type === 'ACCOUNT') {
          ctx.beginPath();
          ctx.arc(x, y, r + 3, 0, 2 * Math.PI);
          ctx.strokeStyle = '#F04438';
          ctx.lineWidth = 2 / k;
          ctx.stroke();
        }
        if (n.vertex_id === focus) {
          ctx.beginPath();
          ctx.arc(x, y, r + 7, 0, 2 * Math.PI);
          ctx.strokeStyle = '#E6EDF3';
          ctx.lineWidth = 1 / k;
          ctx.stroke();
        }
      }
    }
    sim.on('tick', draw);
    const resize = new ResizeObserver(() => {
      const b = el.getBoundingClientRect();
      w = b.width;
      h = b.height;
      const d = Math.min(devicePixelRatio, 2);
      el.width = Math.round(w * d);
      el.height = Math.round(h * d);
      draw();
    });
    resize.observe(el);
    reset.current = () => {
      auto = true;
      px = py = 0;
      draw();
    };
    const wheel = (e: WheelEvent) => {
      e.preventDefault();
      auto = false;
      const old = k;
      k = Math.max(0.025, Math.min(6, k * Math.exp(-e.deltaY * 0.0015)));
      const b = el.getBoundingClientRect(),
        cx = e.clientX - b.left - w / 2,
        cy = e.clientY - b.top - h / 2;
      px = cx - ((cx - px) * k) / old;
      py = cy - ((cy - py) * k) / old;
      draw();
    };
    const down = (e: PointerEvent) => {
      drag = true;
      auto = false;
      x = e.clientX;
      y = e.clientY;
      el.setPointerCapture(e.pointerId);
    };
    const move = (e: PointerEvent) => {
      if (drag) {
        px += e.clientX - x;
        py += e.clientY - y;
        x = e.clientX;
        y = e.clientY;
        draw();
      }
    };
    const up = () => {
      drag = false;
    };
    el.addEventListener('wheel', wheel, { passive: false });
    el.addEventListener('pointerdown', down);
    el.addEventListener('pointermove', move);
    el.addEventListener('pointerup', up);
    el.addEventListener('pointercancel', up);
    return () => {
      sim.stop();
      resize.disconnect();
      el.removeEventListener('wheel', wheel);
      el.removeEventListener('pointerdown', down);
      el.removeEventListener('pointermove', move);
      el.removeEventListener('pointerup', up);
      el.removeEventListener('pointercancel', up);
    };
  }, [graph, focus]);
  return (
    <section className="network">
      <header>
        <h3>Component graph</h3>
        <button
          className="icon"
          aria-label="Fit graph"
          onClick={() => reset.current()}
        >
          <Focus size={17} />
        </button>
      </header>
      <div className="plot">
        <canvas
          ref={canvas}
          aria-label={graph.vertices.length + ' vertices in component graph'}
          data-vertices={graph.vertices.length}
        />
      </div>
      {graph.truncated && (
        <p className="banner">
          +{BigInt(graph.total_vertices) - BigInt(graph.vertices.length) + ''}{' '}
          more vertices
        </p>
      )}
      <footer className="legend">
        <span>
          <i className="circle" />
          Account
        </span>
        <span>
          <i className="square" />
          Card
        </span>
        <span>
          <i className="diamond" />
          Device
        </span>
        <span>
          <i className="ring" />
          Flagged
        </span>
      </footer>
    </section>
  );
}
