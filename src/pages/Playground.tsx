import { useState } from "react";
import { Link } from "react-router-dom";
import { Button } from "@/components/ui/button";
import CodeEditor from "@/components/CodeEditor";
import { 
  Play, 
  RotateCcw, 
  Flame,
  CheckCircle2,
  XCircle,
  Loader2,
  Home
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useToast } from "@/hooks/use-toast";

const DEFAULT_CODE = `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("Playground").getOrCreate()

# Create sample data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# Show the DataFrame
df.show()

# Your code here...
`;

const Playground = () => {
  const { toast } = useToast();
  const [code, setCode] = useState(DEFAULT_CODE);
  const [isRunning, setIsRunning] = useState(false);
  const [output, setOutput] = useState("");
  const [result, setResult] = useState<{
    status: "success" | "error" | null;
    message: string;
  }>({ status: null, message: "" });

  const handleRun = async () => {
    setIsRunning(true);
    setResult({ status: null, message: "" });
    setOutput("Running PySpark code...");

    try {
      const response = await fetch("http://localhost:8080/execute", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ code: code }),
      });

      const data = await response.json();

      if (data.stdout) {
        setOutput(data.stdout);
        setResult({
          status: "success",
          message: "Code executed successfully!"
        });
        toast({
          title: "Success!",
          description: "Your code ran successfully.",
        });
      } else if (data.stderr) {
        setOutput(data.stderr);
        setResult({
          status: "error",
          message: "Execution error - check the output below."
        });
      } else if (data.error) {
        setOutput(`System Error: ${data.error}`);
        setResult({
          status: "error",
          message: "System error occurred."
        });
      }
    } catch (error) {
      setOutput("Failed to connect to Docker Engine. Is it running on localhost:8080?");
      setResult({
        status: "error",
        message: "Could not connect to PySpark execution engine."
      });
      console.error(error);
    } finally {
      setIsRunning(false);
    }
  };

  const handleReset = () => {
    setCode(DEFAULT_CODE);
    setResult({ status: null, message: "" });
    setOutput("");
    toast({
      title: "Code Reset",
      description: "Code has been reset to the default template.",
    });
  };

  return (
    <div className="flex h-screen flex-col bg-background">
      {/* Header */}
      <header className="flex h-12 items-center justify-between border-b border-border px-4">
        <div className="flex items-center gap-4">
          <Link to="/" className="flex items-center gap-2 text-muted-foreground hover:text-foreground transition-colors">
            <Flame className="h-5 w-5 text-primary" />
            <span className="font-semibold text-foreground">SparkLab</span>
          </Link>
          <span className="text-muted-foreground">/</span>
          <span className="text-sm font-medium">Playground</span>
        </div>

        <div className="flex items-center gap-2">
          <Button variant="ghost" size="sm" asChild>
            <Link to="/problems">
              <Home className="h-4 w-4 mr-2" />
              Problems
            </Link>
          </Button>
        </div>
      </header>

      {/* Main Content */}
      <div className="flex flex-1 min-h-0 flex-col">
        {/* Editor Toolbar */}
        <div className="flex items-center justify-between border-b border-border px-4 py-2">
          <div className="flex items-center gap-2">
            <span className="text-xs text-muted-foreground">PySpark Playground</span>
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="ghost"
              size="sm"
              onClick={handleReset}
              className="text-muted-foreground hover:text-foreground"
            >
              <RotateCcw className="mr-2 h-4 w-4" />
              Reset
            </Button>
            <Button
              size="sm"
              onClick={handleRun}
              disabled={isRunning}
              className="bg-primary hover:bg-primary/90"
            >
              {isRunning ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <Play className="mr-2 h-4 w-4" />
              )}
              Run Code
            </Button>
          </div>
        </div>

        {/* Code Editor */}
        <div className="flex-1 min-h-0">
          <CodeEditor
            value={code}
            onChange={(value) => setCode(value || "")}
          />
        </div>

        {/* Output Panel */}
        {(output || result.status) && (
          <div className="border-t border-border flex flex-col max-h-[300px]">
            {/* Status Header */}
            {result.status && (
              <div className={cn(
                "p-3 border-b border-border",
                result.status === "success" ? "bg-success/10" : "bg-destructive/10"
              )}>
                <div className="flex items-center gap-2">
                  {result.status === "success" ? (
                    <CheckCircle2 className="h-5 w-5 text-success" />
                  ) : (
                    <XCircle className="h-5 w-5 text-destructive" />
                  )}
                  <span className={cn(
                    "font-medium",
                    result.status === "success" ? "text-success" : "text-destructive"
                  )}>
                    {result.status === "success" ? "Executed Successfully" : "Execution Failed"}
                  </span>
                </div>
                <p className="mt-1 text-sm text-muted-foreground">
                  {result.message}
                </p>
              </div>
            )}
            
            {/* Output Content */}
            {output && (
              <div className="flex-1 overflow-auto p-4 bg-card">
                <div className="text-xs text-muted-foreground mb-2 font-medium">Output:</div>
                <pre className="text-sm font-mono whitespace-pre-wrap text-foreground">
                  {output}
                </pre>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default Playground;