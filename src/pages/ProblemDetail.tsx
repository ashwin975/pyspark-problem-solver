import { useState } from "react";
import { useParams, Link } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { 
  ResizableHandle, 
  ResizablePanel, 
  ResizablePanelGroup 
} from "@/components/ui/resizable";
import CodeEditor from "@/components/CodeEditor";
import ProblemDescription from "@/components/ProblemDescription";
import { getProblemById, problems } from "@/data/problems";
import { 
  Play, 
  RotateCcw, 
  ChevronLeft, 
  ChevronRight,
  Flame,
  CheckCircle2,
  XCircle,
  Loader2
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useToast } from "@/hooks/use-toast";

const ProblemDetail = () => {
  const { id } = useParams<{ id: string }>();
  const problem = getProblemById(id || "");
  const { toast } = useToast();
  
  const [code, setCode] = useState(problem?.starterCode || "");
  const [isRunning, setIsRunning] = useState(false);
  const [result, setResult] = useState<{
    status: "success" | "error" | null;
    message: string;
  }>({ status: null, message: "" });

  if (!problem) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background">
        <div className="text-center">
          <h1 className="text-2xl font-bold mb-4">Problem Not Found</h1>
          <Button asChild>
            <Link to="/problems">Back to Problems</Link>
          </Button>
        </div>
      </div>
    );
  }

  const currentIndex = problems.findIndex(p => p.id === id);
  const prevProblem = currentIndex > 0 ? problems[currentIndex - 1] : null;
  const nextProblem = currentIndex < problems.length - 1 ? problems[currentIndex + 1] : null;

  const handleRun = async () => {
    setIsRunning(true);
    setResult({ status: null, message: "" });

    // Simulate code execution (in real app, this would call a backend service)
    await new Promise(resolve => setTimeout(resolve, 1500));

    // More realistic validation for PySpark code
    const trimmedCode = code.trim();
    const hasReturn = trimmedCode.includes("return");
    const hasDefEtl = trimmedCode.includes("def etl") || trimmedCode.includes("def solution");
    const isNotJustStarterCode = trimmedCode !== problem.starterCode.trim();
    const hasActualLogic = !trimmedCode.includes("pass") || trimmedCode.split("pass").length > 2 || (trimmedCode.includes("pass") && hasReturn);
    
    // Check if user has written actual code beyond the starter template
    const hasImplementation = hasReturn && hasDefEtl && isNotJustStarterCode && !trimmedCode.endsWith("pass");
    
    if (hasImplementation) {
      setResult({
        status: "success",
        message: "All test cases passed! Great job!"
      });
      toast({
        title: "Success!",
        description: "Your solution passed all test cases.",
      });
    } else {
      let errorMessage = "Some test cases failed. Check your implementation.";
      if (!hasReturn) {
        errorMessage = "Your solution needs to return a value. Did you forget the return statement?";
      } else if (trimmedCode.endsWith("pass")) {
        errorMessage = "Replace 'pass' with your implementation logic.";
      } else if (!isNotJustStarterCode) {
        errorMessage = "Add your implementation to the starter code.";
      }
      setResult({
        status: "error",
        message: errorMessage
      });
    }
    
    setIsRunning(false);
  };

  const handleReset = () => {
    setCode(problem.starterCode);
    setResult({ status: null, message: "" });
    toast({
      title: "Code Reset",
      description: "Your code has been reset to the starter template.",
    });
  };

  return (
    <div className="flex h-screen flex-col bg-background">
      {/* Header */}
      <header className="flex h-12 items-center justify-between border-b border-border px-4">
        <div className="flex items-center gap-4">
          <Link to="/problems" className="flex items-center gap-2 text-muted-foreground hover:text-foreground transition-colors">
            <Flame className="h-5 w-5 text-primary" />
            <span className="font-semibold text-foreground">SparkLab</span>
          </Link>
          <span className="text-muted-foreground">/</span>
          <span className="text-sm font-medium">{problem.title}</span>
        </div>

        <div className="flex items-center gap-2">
          {prevProblem && (
            <Button variant="ghost" size="sm" asChild>
              <Link to={`/problem/${prevProblem.id}`}>
                <ChevronLeft className="h-4 w-4" />
                Prev
              </Link>
            </Button>
          )}
          {nextProblem && (
            <Button variant="ghost" size="sm" asChild>
              <Link to={`/problem/${nextProblem.id}`}>
                Next
                <ChevronRight className="h-4 w-4" />
              </Link>
            </Button>
          )}
        </div>
      </header>

      {/* Main Content */}
      <ResizablePanelGroup direction="horizontal" className="flex-1 min-h-0">
        {/* Left Panel - Problem Description */}
        <ResizablePanel defaultSize={40} minSize={25} maxSize={60}>
          <ProblemDescription problem={problem} />
        </ResizablePanel>

        <ResizableHandle withHandle />

        {/* Right Panel - Code Editor */}
        <ResizablePanel defaultSize={60}>
          <div className="flex h-full min-h-0 flex-col">
            {/* Editor Toolbar */}
            <div className="flex items-center justify-between border-b border-border px-4 py-2">
              <div className="flex items-center gap-2">
                <span className="text-xs text-muted-foreground">Python</span>
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

            {/* Result Panel */}
            {result.status && (
              <div className={cn(
                "border-t border-border p-4",
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
                    {result.status === "success" ? "Accepted" : "Wrong Answer"}
                  </span>
                </div>
                <p className="mt-2 text-sm text-muted-foreground">
                  {result.message}
                </p>
              </div>
            )}
          </div>
        </ResizablePanel>
      </ResizablePanelGroup>
    </div>
  );
};

export default ProblemDetail;
