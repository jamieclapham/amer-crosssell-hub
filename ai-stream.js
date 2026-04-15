// Shared SSE streaming utility for Claude AI calls via Quick's AI endpoint.
// Shared by ai-priorities.js, overview.js (MEDDIC scoring), and ai-tools.
// IMPORTANT: Quick AI requires stream: true — non-streaming returns invalid responses.

export async function streamAI(prompt, { maxTokens = 2000, model = 'claude-sonnet-4-6', timeoutMs = 120_000 } = {}) {
  const controller = new AbortController();
  const resp = await fetch(`${window.location.origin}/api/ai/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model,
      max_tokens: maxTokens,
      messages: [{ role: 'user', content: prompt }],
      stream: true,
    }),
    signal: controller.signal,
  });
  if (!resp.ok) throw new Error(`AI request failed: ${resp.status}`);

  let text = '';
  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  let timerId;
  while (true) {
    // Reset timeout on each chunk — only fires if no data arrives for timeoutMs
    clearTimeout(timerId);
    const timeout = new Promise((_, reject) => {
      timerId = setTimeout(() => { controller.abort(); reject(new Error('AI stream timed out')); }, timeoutMs);
    });
    let done, value;
    try {
      ({ done, value } = await Promise.race([reader.read(), timeout]));
    } catch (err) {
      clearTimeout(timerId);
      throw err;
    }
    clearTimeout(timerId);
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop(); // Keep incomplete line in buffer
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed.startsWith('data:') || trimmed === 'data: [DONE]') continue;
      const payload = trimmed.slice(trimmed.indexOf(':') + 1).trim();
      if (payload === '[DONE]') continue;
      try {
        const chunk = JSON.parse(payload);
        const delta = chunk.choices?.[0]?.delta?.content;
        if (delta) text += delta;
      } catch (_) {}
    }
  }
  // Process remaining buffer
  if (buffer.trim().startsWith('data:') && buffer.trim() !== 'data: [DONE]') {
    const payload = buffer.trim().slice(buffer.trim().indexOf(':') + 1).trim();
    try {
      const chunk = JSON.parse(payload);
      const delta = chunk.choices?.[0]?.delta?.content;
      if (delta) text += delta;
    } catch (_) {}
  }
  return text.trim();
}
