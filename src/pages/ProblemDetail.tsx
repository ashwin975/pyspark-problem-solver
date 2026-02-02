import { useState, useEffect } from "react";
import { useParams, Link } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { 
  ResizableHandle, 
  ResizablePanel, 
  ResizablePanelGroup 
} from "@/components/ui/resizable";
import CodeEditor from "@/components/CodeEditor";
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
import DifficultyBadge from "@/components/DifficultyBadge";
import { FormattedDescription } from "@/components/MarkdownTable";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Lightbulb, ChevronDown, ChevronUp } from "lucide-react";

interface Problem {
  id: string;
  title: string;
  difficulty: "Easy" | "Medium" | "Hard";
  category: string;
  description: string;
  starter_code: string;
  test_setup: string;
  test_validation: string;
  hints: string[];
  solution: string;
}

const ProblemDetail = () => {
  const { id } = useParams<{ id: string }>();
  const { toast } = useToast();
  
  const [problem, setProblem] = useState<Problem | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [code, setCode] = useState("");
  const [isRunning, setIsRunning] = useState(false);
  const [output, setOutput] = useState("");
  const [result, setResult] = useState<{
    status: "success" | "error" | null;
    message: string;
  }>({ status: null, message: "" });
  const [revealedHints, setRevealedHints] = useState<number[]>([]);

  // Fetch problem data
  useEffect(() => {
    const fetchProblem = async () => {
      setLoading(true);
      setError(null);
      try {
        const response = await fetch(`/problems/${id}.json`);
        if (!response.ok) {
          throw new Error("Problem not found");
        }
        const data: Problem = await response.json();
        setProblem(data);
        setCode(data.starter_code);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load problem");
      } finally {
        setLoading(false);
      }
    };

    if (id) {
      fetchProblem();
    }
  }, [id]);

  const toggleHint = (index: number) => {
    setRevealedHints(prev => 
      prev.includes(index) 
        ? prev.filter(i => i !== index)
        : [...prev, index]
    );
  };

  const handleRun = async () => {
    if (!problem) return;
    
    setIsRunning(true);
    setResult({ status: null, message: "" });
    setOutput("Running PySpark code...");

    // Combine test_setup + user code + test_validation
    const fullScript = `${problem.test_setup}

# User Solution
${code}

# Test Validation
${problem.test_validation}`;

    try {
      const response = await fetch("http://localhost:8080/execute", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ code: fullScript }),
      });

      const data = await response.json();

      if (data.stdout) {
        setOutput(data.stdout);
        const passed = data.stdout.includes("✅ TEST PASSED!");
        setResult({
          status: passed ? "success" : "error",
          message: passed ? "All tests passed!" : "Check the output for details."
        });
        if (passed) {
          toast({
            title: "Success!",
            description: "All tests passed!",
          });
        }
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
    if (problem) {
      setCode(problem.starter_code);
      setResult({ status: null, message: "" });
      setOutput("");
      toast({
        title: "Code Reset",
        description: "Your code has been reset to the starter template.",
      });
    }
  };

  if (loading) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  if (error || !problem) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background">
        <div className="text-center">
          <h1 className="text-2xl font-bold mb-4">Problem Not Found</h1>
          <p className="text-muted-foreground mb-4">{error}</p>
          <Button asChild>
            <Link to="/problems">Back to Problems</Link>
          </Button>
        </div>
      </div>
    );
  }

  const currentId = parseInt(id || "1");
  const prevId = currentId > 1 ? currentId - 1 : null;
  const nextId = currentId + 1; // We'll handle 404 on the next page if it doesn't exist

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
          {prevId && (
            <Button variant="ghost" size="sm" asChild>
              <Link to={`/problems/${prevId}`}>
                <ChevronLeft className="h-4 w-4" />
                Prev
              </Link>
            </Button>
          )}
          <Button variant="ghost" size="sm" asChild>
            <Link to={`/problems/${nextId}`}>
              Next
              <ChevronRight className="h-4 w-4" />
            </Link>
          </Button>
        </div>
      </header>

      {/* Main Content */}
      <ResizablePanelGroup direction="horizontal" className="flex-1 min-h-0">
        {/* Left Panel - Problem Description */}
        <ResizablePanel defaultSize={40} minSize={25} maxSize={60}>
          <div className="flex h-full flex-col overflow-hidden">
            <Tabs defaultValue="description" className="flex h-full flex-col">
              <TabsList className="w-full justify-start rounded-none border-b bg-transparent px-4">
                <TabsTrigger 
                  value="description" 
                  className="rounded-none border-b-2 border-transparent data-[state=active]:border-primary data-[state=active]:bg-transparent"
                >
                  Description
                </TabsTrigger>
                <TabsTrigger 
                  value="hints"
                  className="rounded-none border-b-2 border-transparent data-[state=active]:border-primary data-[state=active]:bg-transparent"
                >
                  Hints ({problem.hints.length})
                </TabsTrigger>
                <TabsTrigger 
                  value="solution"
                  className="rounded-none border-b-2 border-transparent data-[state=active]:border-primary data-[state=active]:bg-transparent"
                >
                  Solution
                </TabsTrigger>
              </TabsList>

              <TabsContent value="description" className="flex-1 overflow-auto p-4">
                <div className="space-y-4">
                  <div className="flex items-center justify-between">
                    <h2 className="text-xl font-bold">{problem.title}</h2>
                    <DifficultyBadge difficulty={problem.difficulty} />
                  </div>
                  
                  <div className="inline-block rounded-full bg-secondary px-3 py-1 text-xs font-medium text-muted-foreground">
                    {problem.category}
                  </div>

                  <FormattedDescription text={problem.description} />
                </div>
              </TabsContent>

              <TabsContent value="hints" className="flex-1 overflow-auto p-4">
                <div className="space-y-3">
                  <p className="text-sm text-muted-foreground mb-4">
                    Click on a hint to reveal it. Try to solve the problem before looking at hints!
                  </p>
                  {problem.hints.map((hint, index) => (
                    <Card 
                      key={index}
                      className={cn(
                        "cursor-pointer transition-all",
                        revealedHints.includes(index) 
                          ? "border-primary/50" 
                          : "hover:border-primary/30"
                      )}
                      onClick={() => toggleHint(index)}
                    >
                      <CardHeader className="py-3 px-4">
                        <div className="flex items-center justify-between">
                          <div className="flex items-center gap-2">
                            <Lightbulb className={cn(
                              "h-4 w-4",
                              revealedHints.includes(index) ? "text-accent" : "text-muted-foreground"
                            )} />
                            <span className="text-sm font-medium">Hint {index + 1}</span>
                          </div>
                          {revealedHints.includes(index) ? (
                            <ChevronUp className="h-4 w-4 text-muted-foreground" />
                          ) : (
                            <ChevronDown className="h-4 w-4 text-muted-foreground" />
                          )}
                        </div>
                      </CardHeader>
                      {revealedHints.includes(index) && (
                        <CardContent className="pt-0 pb-3 px-4">
                          <p className="text-sm text-foreground/80">{hint}</p>
                        </CardContent>
                      )}
                    </Card>
                  ))}
                </div>
              </TabsContent>

              <TabsContent value="solution" className="flex-1 overflow-auto p-4">
                <div className="space-y-4">
                  <div className="rounded-lg bg-accent/10 border border-accent/30 p-4">
                    <div className="flex items-center gap-2 mb-2">
                      <Lightbulb className="h-4 w-4 text-accent" />
                      <span className="text-sm font-medium text-accent">Pro Tip</span>
                    </div>
                    <p className="text-sm text-muted-foreground">
                      Try solving the problem yourself first! Looking at solutions too early can hinder your learning.
                    </p>
                  </div>
                  <div className="rounded-lg bg-muted border border-border overflow-hidden">
                    <div className="border-b border-border bg-secondary px-4 py-2 text-xs text-muted-foreground">
                      solution.py
                    </div>
                    <pre className="p-4 text-sm overflow-x-auto bg-card">
                      <code className="text-foreground font-mono whitespace-pre-wrap">
                        {problem.solution}
                      </code>
                    </pre>
                  </div>
                </div>
              </TabsContent>
            </Tabs>
          </div>
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
                        {result.status === "success" ? "Tests Passed" : "Tests Failed"}
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
        </ResizablePanel>
      </ResizablePanelGroup>
    </div>
  );
};

export default ProblemDetail;
