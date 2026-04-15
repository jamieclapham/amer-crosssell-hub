// Tiering Hub — Automatic account tiering based on rules
export function renderTiering(data, user) {
  const el = document.getElementById('tab-tiering');
  if (!el) return;

  el.innerHTML = `
    <div class="card">
      <div class="card-title">Account Tiering</div>
      <p style="color:var(--text-secondary);margin-top:8px">
        Automatic account tiering based on defined rules. Coming soon.
      </p>
    </div>
  `;
}
