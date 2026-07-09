// Ambient 3D hero for the landing page: a slow-drifting field of segment bubbles
// with the vectors that live inside them. Purely decorative — no interaction, no
// steps — so it reinforces the product idea (vectors grouped into read-once
// bubbles) without competing with the copy. Loaded as an ES module; if three.js
// can't be fetched the hero simply stays empty and the layout is unaffected.

const THREE_URL = "https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.module.js";

const SEGMENT_COLORS = [0x2f7f73, 0xc14d32, 0x6f4a31, 0x3f6b57];
const PAPER = 0xf6f4ee;

// Hand-placed bubbles of varied size, each with a few members inside its radius.
const BUBBLES = [
  { c: [-3.4, 1.4, -0.5], r: 1.5, color: 0 },
  { c: [1.7, 2.2, -1.6], r: 1.1, color: 1 },
  { c: [3.2, -0.6, 0.8], r: 1.7, color: 2 },
  { c: [-1.4, -1.8, 1.4], r: 1.25, color: 3 },
  { c: [0.4, 0.2, 2.6], r: 0.95, color: 0 },
  { c: [-3.0, -1.2, -2.2], r: 1.05, color: 1 },
];

function membersFor(bubble, count, salt) {
  // Deterministic offsets inside the radius (no Math.random for stable frames).
  const out = [];
  for (let i = 0; i < count; i += 1) {
    const a = (i * 1.9 + salt) * 1.3;
    const b = (i * 2.7 + salt) * 0.7;
    const rr = bubble.r * (0.35 + (0.5 * ((i * 7 + salt) % 5)) / 5);
    out.push([
      bubble.c[0] + rr * Math.cos(a) * Math.cos(b),
      bubble.c[1] + rr * Math.sin(b),
      bubble.c[2] + rr * Math.sin(a) * Math.cos(b),
    ]);
  }
  return out;
}

export async function initHero3d() {
  const mount = document.querySelector("[data-hero3d]");
  if (!mount) return;

  let THREE;
  try {
    THREE = await import(/* @vite-ignore */ THREE_URL);
  } catch {
    return; // No network for three.js — leave the hero clean.
  }

  const scene = new THREE.Scene();
  const world = new THREE.Group();
  scene.add(world);

  scene.add(new THREE.HemisphereLight(0xffffff, 0xcfd3c2, 1.15));
  const key = new THREE.DirectionalLight(0xffffff, 1.3);
  key.position.set(4, 7, 6);
  scene.add(key);
  const warm = new THREE.DirectionalLight(0xffd9b0, 0.55);
  warm.position.set(-6, -3, -2);
  scene.add(warm);

  const pointGeo = new THREE.SphereGeometry(0.12, 20, 20);
  BUBBLES.forEach((bubble, index) => {
    const color = SEGMENT_COLORS[bubble.color];
    const group = new THREE.Group();
    group.position.set(bubble.c[0], bubble.c[1], bubble.c[2]);

    const shell = new THREE.Mesh(
      new THREE.SphereGeometry(bubble.r, 36, 36),
      new THREE.MeshStandardMaterial({
        color,
        transparent: true,
        opacity: 0.09,
        roughness: 0.7,
        side: THREE.DoubleSide,
        depthWrite: false,
      }),
    );
    const wire = new THREE.LineSegments(
      new THREE.WireframeGeometry(new THREE.SphereGeometry(bubble.r, 14, 10)),
      new THREE.LineBasicMaterial({ color, transparent: true, opacity: 0.22 }),
    );
    group.add(shell, wire);

    membersFor(bubble, 4 + (index % 3), index).forEach((pos) => {
      const mesh = new THREE.Mesh(
        pointGeo,
        new THREE.MeshStandardMaterial({ color, roughness: 0.3, metalness: 0.1 }),
      );
      // positions are absolute; group is at bubble centre, so localise them.
      mesh.position.set(pos[0] - bubble.c[0], pos[1] - bubble.c[1], pos[2] - bubble.c[2]);
      group.add(mesh);
    });

    world.add(group);
  });

  const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
  renderer.setClearColor(PAPER, 0); // transparent so it floats on the hero
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
  mount.appendChild(renderer.domElement);
  renderer.domElement.style.display = "block";
  renderer.domElement.style.width = "100%";
  renderer.domElement.setAttribute("aria-hidden", "true");

  const camera = new THREE.PerspectiveCamera(40, 1, 0.1, 100);
  const camRadius = 11.5;

  const resize = () => {
    const width = mount.clientWidth || 560;
    const height = mount.clientHeight || Math.round(width * 0.9);
    renderer.setSize(width, height, false);
    camera.aspect = width / height;
    camera.updateProjectionMatrix();
  };
  resize();
  if (typeof ResizeObserver !== "undefined") new ResizeObserver(resize).observe(mount);

  const reduce =
    typeof window.matchMedia === "function" &&
    window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  let yaw = 0.5;
  const target = new THREE.Vector3(0, 0, 0);
  const animate = () => {
    if (!reduce) yaw += 0.0016;
    world.rotation.y = yaw;
    world.rotation.x = Math.sin(yaw * 0.5) * 0.12;
    camera.position.set(0, 1.2, camRadius);
    camera.lookAt(target);
    renderer.render(scene, camera);
    requestAnimationFrame(animate);
  };
  animate();
}

if (typeof document !== "undefined") {
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initHero3d);
  } else {
    initHero3d();
  }
}
