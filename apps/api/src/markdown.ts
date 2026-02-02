import { unified } from 'unified';
import remarkParse from 'remark-parse';

export function markdownToText(markdown: string) {
  if (!markdown) return '';

  const tree = unified().use(remarkParse).parse(markdown);

  const renderInline = (node: any): string => {
    switch (node.type) {
      case 'text':
        return node.value ?? '';
      case 'inlineCode':
        return node.value ?? '';
      case 'break':
        return '\n';
      case 'emphasis':
      case 'strong':
      case 'delete':
        return (node.children ?? []).map(renderInline).join('');
      case 'link': {
        const label = (node.children ?? []).map(renderInline).join('').trim();
        const url = (node.url ?? '').trim();
        if (!url) return label;
        if (!label || label === url) return url;
        return `${label} (${url})`;
      }
      case 'image': {
        const alt = (node.alt ?? 'image').trim();
        const url = (node.url ?? '').trim();
        if (!url) return alt;
        return `${alt} (${url})`;
      }
      default:
        if (node.children) {
          return node.children.map(renderInline).join('');
        }
        return '';
    }
  };

  const renderBlock = (node: any): string => {
    switch (node.type) {
      case 'root':
        return (node.children ?? []).map(renderBlock).filter(Boolean).join('\n\n');
      case 'paragraph':
      case 'heading':
        return (node.children ?? []).map(renderInline).join('');
      case 'code':
        return node.value ?? '';
      case 'list':
        return (node.children ?? []).map(renderBlock).filter(Boolean).join('\n');
      case 'listItem':
        return (node.children ?? []).map(renderBlock).filter(Boolean).join('\n');
      case 'blockquote':
        return (node.children ?? []).map(renderBlock).filter(Boolean).join('\n');
      case 'thematicBreak':
        return '';
      default:
        return renderInline(node);
    }
  };

  const raw = renderBlock(tree);
  return raw
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}
