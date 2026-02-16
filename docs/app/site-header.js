// ./scripts/site-header.js
(() => {
  "use strict";

  // --- Your config (as you provided) ---
  const HEADER_CONFIG = {
    brand: {
      mainLogo: { href: "/", src: "./images/main-logo.png", alt: "Your org/site" },
      toolLogoByPageId: {
        "ontoeagle": { src: "./images/Eagle-VI_1753264913.svg", alt: "OntoEagle Semantic Lookup" },
        "iri-registry": { src: "./images/iri-registry.svg", alt: "IRI Registry" },
        "ontology-tabulator": { src: "./images/ontology-tabulator.svg", alt: "Ontology Tabulator" },
        "cq-ferret": { src: "./images/cq-ferret.svg", alt: "CQ Ferret" },
        "bp-weaver": { src: "./images/bp-weaver.svg", alt: "BP Weaver" },
        "controlled-vocabulary-registry": { src: "./images/controlled-vocabulary-registry.svg", alt: "Controlled Vocabulary Registry" },
        "tom": { src: "./images/tom.svg", alt: "Tabular Ontology Maker" },
        "table-nova": { src: "./images/table-nova-logo.svg", alt: "Table Nova" },
        "shacl-generator": { src: "./images/shacl-generator.svg", alt: "SHACL Generator" },
        "axiolotl": { src: "./images/axiolotl.svg", alt: "Axiolotl SPARQL & Inference" },
        "sparql-pattern-visualizer": { src: "./images/sparql-pattern-visualizer.svg", alt: "SPARQL Pattern Visualizer" },
        "ontology-curation-manager": { src: "./images/ontology-curation-manager.svg", alt: "Ontology Curation Manager" },
        "myna-iri-swapper": { src: "./images/myna-iri-swapper.svg", alt: "Myna IRI Swapper" },
        "visual-lynx": { src: "./images/visual-lynx.svg", alt: "Visual Lynx" },
      },
      defaultToolLogo: { src: "./images/default.svg", alt: "Tool" }
    },

    groups: [
      {
        title: "Data Exploration",
        items: [
          { label: "OntoEagle", href: "/search.html", pageId: "ontoeagle" },
        //  { label: "IRI Registry", href: "/iri-registry.html", pageId: "iri-registry" },
          { label: "Ontology Tabulator", href: "https://jonathanvajda.github.io/ontology-tabulator/", pageId: "ontology-tabulator" },
          { label: "Visual Lynx", href: "https://jonathanvajda.github.io/visual-lynx/", pageId: "visual-lynx" },
        ],
      },
{
        title: "Domain Analysis",
        items: [
    /**          { label: "Competency Question Ferret", href: "/cq-ferret.html", pageId: "cq-ferret" },
          { label: "Business Process Weaver", href: "/bp-weaver.html", pageId: "bp-weaver" },*/
           { label: "Mermaid Diagram Builder 🔗", href: "https://skreen5hot.github.io/mermaid/", pageId: "mermaid-diagram-builder" },
        ],
      },
      {
        title: "Building Tools",
        items: [
        //  { label: "Controlled Vocabulary", href: "/controlled-vocabulary-registry.html", pageId: "controlled-vocabulary-registry" },
          { label: "Tabular Ontology Maker (TOM)", href: "https://jonathanvajda.github.io/tabular-ontology-maker/", pageId: "tom" },
          { label: "Table Nova", href: "https://jonathanvajda.github.io/table-nova/", pageId: "table-nova" },
          { label: "Knowledge Graph Modeler 🔗", href: "https://skreen5hot.github.io/kgModeler/", pageId: "kg-modeler" },
        //  { label: "SHACL Generator", href: "/shacl-generator.html", pageId: "shacl-generator" },
        ],
      },
      {
      title: "Data Manipulation",
        items: [
          { label: "Axiolotl SPARQL & Inference", href: "https://jonathanvajda.github.io/axiolotl/", pageId: "axiolotl" },
          { label: "SPARQL Pattern Visualizer", href: "https://jonathanvajda.github.io/sparql-pattern-visualizer/", pageId: "sparql-pattern-visualizer" },
        ],
      },
      {
        title: "Maintenance",
            items: [
            { label: "Ontology Curation Manager", href: "https://jonathanvajda.github.io/ontology-curation-manager/", pageId: "ontology-curation-manager" },
            { label: "Myna IRI Swapper", href: "https://jonathanvajda.github.io/iri-swapper/", pageId: "myna-iri-swapper" },
            ],
        },
        ],
    };

  function escapeHtml(s) {
    return String(s)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function getPageId() {
    const pid = document.body?.getAttribute("data-page-id");
    return pid && pid.trim() ? pid.trim() : null;
  }

  function pickToolLogo(pageId) {
    const map = HEADER_CONFIG.brand?.toolLogoByPageId || {};
    const fallback = HEADER_CONFIG.brand?.defaultToolLogo || { src: "", alt: "" };
    return (pageId && map[pageId]) ? map[pageId] : fallback;
  }

  function buildSectionsHtml(currentPageId) {
    const groups = Array.isArray(HEADER_CONFIG.groups) ? HEADER_CONFIG.groups : [];
    if (groups.length === 0) return "";

    const sections = groups.map((g) => {
      const title = escapeHtml(g.title || "");
      const items = Array.isArray(g.items) ? g.items : [];

      const links = items.map((it) => {
        const active = currentPageId && it.pageId === currentPageId;
        return `
          <li>
            <a class="sitehdr-link${active ? " is-active" : ""}"
               href="${escapeHtml(it.href || "#")}"
               ${active ? 'aria-current="page"' : ""}>
              ${escapeHtml(it.label || "")}
            </a>
          </li>
        `;
      }).join("");

      return `
        <section class="sitehdr-section" aria-label="${title}">
          <h2 class="sitehdr-section__title">${title}</h2>
          <ul class="sitehdr-section__list">
            ${links}
          </ul>
        </section>
      `;
    }).join("");

    return `<nav class="sitehdr-sections" aria-label="Tool sections">${sections}</nav>`;
  }

  function renderHeader() {
    const mount = document.getElementById("siteHeader");
    if (!mount) return;

    const pageId = getPageId();
    const toolLogo = pickToolLogo(pageId);

    const mainLogo = HEADER_CONFIG.brand?.mainLogo || { href: "/", src: "", alt: "" };

    // IMPORTANT: menu is INSIDE sitehdr-bar as the 3rd sibling => renders to the right
    mount.innerHTML = `
      <div class="sitehdr">
        <div class="sitehdr-bar">
          <a class="sitehdr-brand" href="${escapeHtml(mainLogo.href)}">
            <img class="sitehdr-brand__main"
                 src="${escapeHtml(mainLogo.src)}"
                 alt="${escapeHtml(mainLogo.alt)}" />
          </a>

          <div class="sitehdr-tool">
            <img class="sitehdr-tool__img"
                 src="${escapeHtml(toolLogo.src)}"
                 alt="${escapeHtml(toolLogo.alt)}" />
                 <h1 class="sitehdr-tool__title">${escapeHtml(toolLogo.alt)}</h1>
          </div>

          ${buildSectionsHtml(pageId)}
        </div>
      </div>
    `;
  }

  // script loaded at end of body => DOM is ready
  renderHeader();
})();
