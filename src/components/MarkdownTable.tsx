import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

interface MarkdownTableProps {
  content: string;
}

// Parse markdown table syntax and render as proper table UI
const parseMarkdownTable = (tableString: string) => {
  const lines = tableString.trim().split('\n').filter(line => line.trim());
  
  if (lines.length < 2) return null;
  
  // Parse header row
  const headerLine = lines[0];
  const headers = headerLine
    .split('|')
    .map(cell => cell.trim())
    .filter(cell => cell);
  
  // Skip separator line (index 1)
  // Parse data rows
  const dataRows = lines.slice(2).map(line => 
    line
      .split('|')
      .map(cell => cell.trim())
      .filter(cell => cell)
  );
  
  return { headers, dataRows };
};

const MarkdownTable = ({ content }: MarkdownTableProps) => {
  const parsed = parseMarkdownTable(content);
  
  if (!parsed) return <pre className="text-sm">{content}</pre>;
  
  return (
    <div className="my-3 rounded-lg border border-border overflow-hidden">
      <Table>
        <TableHeader>
          <TableRow className="bg-muted/50">
            {parsed.headers.map((header, i) => (
              <TableHead key={i} className="text-foreground font-semibold">
                {header}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {parsed.dataRows.map((row, rowIndex) => (
            <TableRow key={rowIndex}>
              {row.map((cell, cellIndex) => (
                <TableCell key={cellIndex} className="text-foreground/80 font-mono text-xs">
                  {cell}
                </TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
};

// Component to parse description text and render tables properly
export const FormattedDescription = ({ text }: { text: string }) => {
  // Split by table patterns
  const tableRegex = /(\|[^\n]+\|\n\|[-:\s|]+\|\n(?:\|[^\n]+\|\n?)*)/g;
  
  const parts: { type: 'text' | 'table'; content: string }[] = [];
  let lastIndex = 0;
  let match;
  
  while ((match = tableRegex.exec(text)) !== null) {
    // Add text before the table
    if (match.index > lastIndex) {
      parts.push({
        type: 'text',
        content: text.slice(lastIndex, match.index)
      });
    }
    // Add the table
    parts.push({
      type: 'table',
      content: match[1]
    });
    lastIndex = match.index + match[0].length;
  }
  
  // Add remaining text
  if (lastIndex < text.length) {
    parts.push({
      type: 'text',
      content: text.slice(lastIndex)
    });
  }
  
  return (
    <div className="space-y-2">
      {parts.map((part, index) => {
        if (part.type === 'table') {
          return <MarkdownTable key={index} content={part.content} />;
        }
        
        // Format text content - handle bold, code, etc.
        const formattedContent = part.content
          .split('\n')
          .map((line, lineIndex) => {
            // Bold text
            const boldRegex = /\*\*([^*]+)\*\*/g;
            let formattedLine = line.replace(boldRegex, '<strong class="text-foreground font-semibold">$1</strong>');
            
            // Inline code
            const codeRegex = /`([^`]+)`/g;
            formattedLine = formattedLine.replace(codeRegex, '<code class="bg-muted px-1.5 py-0.5 rounded text-primary font-mono text-xs">$1</code>');
            
            if (line.trim() === '') {
              return <br key={lineIndex} />;
            }
            
            return (
              <span 
                key={lineIndex} 
                className="block text-foreground/90"
                dangerouslySetInnerHTML={{ __html: formattedLine }}
              />
            );
          });
        
        return (
          <div key={index} className="leading-relaxed">
            {formattedContent}
          </div>
        );
      })}
    </div>
  );
};

export default MarkdownTable;
