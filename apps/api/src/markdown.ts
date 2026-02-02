export function markdownToText(markdown: string) {
  if (!markdown) return '';

  const codeBlocks: string[] = [];
  let text = markdown
    .replace(/```[\s\S]*?```/g, (match) => {
      const lines = match.split(/\r?\n/);
      const content = lines.slice(1, -1).join('\n');
      const placeholder = `@@CODEBLOCK${codeBlocks.length}@@`;
      codeBlocks.push(content);
      return `\n${placeholder}\n`;
    })
    .replace(/~~~[\s\S]*?~~~/g, (match) => {
      const lines = match.split(/\r?\n/);
      const content = lines.slice(1, -1).join('\n');
      const placeholder = `@@CODEBLOCK${codeBlocks.length}@@`;
      codeBlocks.push(content);
      return `\n${placeholder}\n`;
    });

  text = text
    .replace(/`([^`]+)`/g, '$1')
    .replace(/!\[([^\]]*)\]\(([^)]+)\)/g, (_, alt, url) => {
      const altText = (alt || '').trim() || 'image';
      const urlText = (url || '').trim();
      return urlText ? `${altText} (${urlText})` : altText;
    })
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_, label, url) => {
      const labelText = (label || '').trim();
      const urlText = (url || '').trim();
      return urlText ? `${labelText} (${urlText})` : labelText;
    })
    .replace(/<((https?:\/\/|mailto:)[^>]+)>/g, '$1')
    .replace(/^\s{0,3}#{1,6}\s+/gm, '')
    .replace(/^\s{0,3}>\s?/gm, '')
    .replace(/^\s{0,3}([-*+])\s+/gm, '')
    .replace(/^\s{0,3}\d+\.\s+/gm, '')
    .replace(/\*\*(.*?)\*\*/g, '$1')
    .replace(/__(.*?)__/g, '$1')
    .replace(/\*(.*?)\*/g, '$1')
    .replace(/_(.*?)_/g, '$1')
    .replace(/~~(.*?)~~/g, '$1')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();

  text = text.replace(/@@CODEBLOCK(\d+)@@/g, (_, idx) => {
    const content = codeBlocks[Number(idx)] ?? '';
    return content;
  });

  return text.trim();
}
