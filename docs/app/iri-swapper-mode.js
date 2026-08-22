const MODE_PARAM = 'mode';
const MODES = new Set(['rdf', 'sparql']);

function getRequestedMode() {
  const params = new URLSearchParams(window.location.search);
  const requested = params.get(MODE_PARAM);
  return MODES.has(requested) ? requested : 'rdf';
}

function setModeUrl(mode) {
  const url = new URL(window.location.href);
  if (mode === 'rdf') {
    url.searchParams.delete(MODE_PARAM);
  } else {
    url.searchParams.set(MODE_PARAM, mode);
  }
  window.location.href = url.toString();
}

function markSelectedMode(mode) {
  document.querySelectorAll('[data-mode-select]').forEach((button) => {
    const isSelected = button.dataset.modeSelect === mode;
    button.classList.toggle('is-active', isSelected);
    button.setAttribute('aria-pressed', String(isSelected));
    button.addEventListener('click', () => {
      const nextMode = button.dataset.modeSelect;
      if (nextMode && nextMode !== mode) setModeUrl(nextMode);
    });
  });
}

async function boot() {
  const mode = getRequestedMode();
  const template = document.getElementById(`${mode}ModeTemplate`);
  const mount = document.getElementById('modeMount');
  if (!template || !mount) {
    throw new Error(`Missing IRI Swapper mode template: ${mode}`);
  }

  mount.replaceChildren(template.content.cloneNode(true));
  document.body.dataset.iriSwapperMode = mode;
  markSelectedMode(mode);

  if (mode === 'sparql') {
    await import('./sparql-iri-swapper.js');
  } else {
    await import('./ont-iri-swapper.js');
  }
}

boot().catch((error) => {
  console.error(error);
  const status = typeof document === 'undefined' ? null : document.getElementById('status');
  if (status) {
    status.textContent = `Startup failed: ${error?.message || error}`;
  }
});
