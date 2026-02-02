import { useState } from "react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Lightbulb, ChevronDown, ChevronUp } from "lucide-react";
import DifficultyBadge from "./DifficultyBadge";
import { FormattedDescription } from "./MarkdownTable";
import type { Problem } from "@/data/problems";
import { cn } from "@/lib/utils";
interface ProblemDescriptionProps {
  problem: Problem;
}

const ProblemDescription = ({ problem }: ProblemDescriptionProps) => {
  const [showHints, setShowHints] = useState(false);
  const [revealedHints, setRevealedHints] = useState<number[]>([]);

  const toggleHint = (index: number) => {
    setRevealedHints(prev => 
      prev.includes(index) 
        ? prev.filter(i => i !== index)
        : [...prev, index]
    );
  };

  return (
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
  );
};

export default ProblemDescription;
