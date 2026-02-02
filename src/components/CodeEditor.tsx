import Editor from "@monaco-editor/react";
import * as React from "react";

interface CodeEditorProps {
  value: string;
  onChange: (value: string | undefined) => void;
  language?: string;
  readOnly?: boolean;
}

const CodeEditor = React.forwardRef<HTMLDivElement, CodeEditorProps>(
  ({ value, onChange, language = "python", readOnly = false }, ref) => {
    return (
      <div ref={ref} className="h-full w-full overflow-hidden rounded-lg border border-border bg-editor-bg">
        <Editor
          height="100%"
          language={language}
          value={value}
          onChange={onChange}
          theme="vs-dark"
          options={{
            fontSize: 14,
            fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
            minimap: { enabled: false },
            scrollBeyondLastLine: false,
            padding: { top: 16, bottom: 16 },
            lineNumbers: "on",
            renderLineHighlight: "line",
            cursorBlinking: "smooth",
            cursorSmoothCaretAnimation: "on",
            smoothScrolling: true,
            tabSize: 4,
            wordWrap: "on",
            automaticLayout: true,
            readOnly,
            scrollbar: {
              vertical: "auto",
              horizontal: "auto",
              useShadows: false,
              verticalScrollbarSize: 8,
              horizontalScrollbarSize: 8,
            },
          }}
        />
      </div>
    );
  },
);
CodeEditor.displayName = "CodeEditor";

export default CodeEditor;
