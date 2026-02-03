import Header from "@/components/Header";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { 
  Github, 
  GitPullRequest, 
  FileCode, 
  CheckCircle2,
  ArrowRight,
  BookOpen,
  Code2,
  MessageSquare
} from "lucide-react";

const Contribute = () => {
  const steps = [
    {
      icon: Github,
      title: "Fork the Repository",
      description: "Start by forking our GitHub repository to your account."
    },
    {
      icon: FileCode,
      title: "Create a Problem",
      description: "Add a new problem file following our template structure."
    },
    {
      icon: CheckCircle2,
      title: "Add Test Cases",
      description: "Include comprehensive test cases to validate solutions."
    },
    {
      icon: GitPullRequest,
      title: "Submit a PR",
      description: "Open a pull request with your contribution for review."
    }
  ];

  const guidelines = [
    {
      icon: Code2,
      title: "Problem Structure",
      points: [
        "Clear problem statement with input/output examples",
        "Well-defined constraints and edge cases",
        "Starter code template with proper type hints",
        "At least 3 test cases (basic, edge, complex)"
      ]
    },
    {
      icon: BookOpen,
      title: "Content Quality",
      points: [
        "Original problems or properly attributed sources",
        "Real-world relevance to data engineering",
        "Progressive difficulty within categories",
        "Clear, concise explanations"
      ]
    },
    {
      icon: MessageSquare,
      title: "Review Process",
      points: [
        "All PRs reviewed within 48 hours",
        "Constructive feedback provided",
        "Contributors credited in problem metadata",
        "Major contributors added to README"
      ]
    }
  ];

  return (
    <div className="min-h-screen bg-background">
      <Header />
      
      <main className="container py-12">
        {/* Hero */}
        <div className="mx-auto max-w-3xl text-center mb-16">
          <div className="mb-6 inline-flex items-center gap-2 rounded-full border border-primary/30 bg-primary/10 px-4 py-1.5 text-sm font-medium text-primary">
            <Github className="h-4 w-4" />
            Open Source
          </div>
          
          <h1 className="text-4xl font-bold mb-4">Contribute to PySpark Katas</h1>
          <p className="text-lg text-muted-foreground mb-8">
            Help the community learn PySpark by contributing problems, solutions, and improvements. 
            Your contributions make a difference!
          </p>

          <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
            <Button size="lg" asChild className="gap-2">
              <a href="https://github.com/ashwin975/pyspark-problem-solver" target="_blank" rel="noopener noreferrer">
                <Github className="h-5 w-5" />
                View on GitHub
              </a>
            </Button>
            <Button size="lg" variant="outline" asChild className="gap-2">
              <a href="https://github.com/ashwin975/pyspark-problem-solver/pulls" target="_blank" rel="noopener noreferrer">
                <GitPullRequest className="h-5 w-5" />
                Submit a Problem
              </a>
            </Button>
          </div>
        </div>

        {/* How to Contribute */}
        <section className="mb-16">
          <h2 className="text-2xl font-bold text-center mb-8">How to Contribute</h2>
          <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
            {steps.map((step, index) => (
              <div key={index} className="relative">
                <Card className="h-full border-border hover:border-primary/30 transition-colors">
                  <CardHeader>
                    <div className="flex items-center gap-4">
                      <div className="flex h-12 w-12 items-center justify-center rounded-full bg-primary/10 text-primary">
                        <step.icon className="h-6 w-6" />
                      </div>
                      <span className="text-3xl font-bold text-muted-foreground/30">
                        {index + 1}
                      </span>
                    </div>
                    <CardTitle className="text-lg">{step.title}</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="text-sm text-muted-foreground">{step.description}</p>
                  </CardContent>
                </Card>
                {index < steps.length - 1 && (
                  <ArrowRight className="hidden lg:block absolute top-1/2 -right-3 h-6 w-6 text-muted-foreground/30 -translate-y-1/2" />
                )}
              </div>
            ))}
          </div>
        </section>

        {/* Guidelines */}
        <section className="mb-16">
          <h2 className="text-2xl font-bold text-center mb-8">Contribution Guidelines</h2>
          <div className="grid gap-6 md:grid-cols-3">
            {guidelines.map((guideline, index) => (
              <Card key={index} className="border-border">
                <CardHeader>
                  <div className="flex items-center gap-3">
                    <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-secondary">
                      <guideline.icon className="h-5 w-5 text-primary" />
                    </div>
                    <CardTitle className="text-lg">{guideline.title}</CardTitle>
                  </div>
                </CardHeader>
                <CardContent>
                  <ul className="space-y-2">
                    {guideline.points.map((point, pointIndex) => (
                      <li key={pointIndex} className="flex items-start gap-2 text-sm text-muted-foreground">
                        <CheckCircle2 className="h-4 w-4 text-primary shrink-0 mt-0.5" />
                        {point}
                      </li>
                    ))}
                  </ul>
                </CardContent>
              </Card>
            ))}
          </div>
        </section>

        {/* Problem Template */}
        <section className="mb-16">
          <h2 className="text-2xl font-bold text-center mb-8">Problem Template</h2>
          <Card className="bg-editor-bg border-border overflow-hidden">
            <div className="border-b border-border px-4 py-2 text-xs text-muted-foreground">
              problem_template.py
            </div>
            <pre className="p-4 text-sm overflow-x-auto">
              <code className="text-foreground/90 font-mono">{`{
  "id": "unique-problem-id",
  "title": "Problem Title",
  "difficulty": "Easy | Medium | Hard",
  "category": "DataFrame Basics | Transformations | ...",
  "description": "Clear problem statement with examples...",
  "starterCode": "def solution(spark: SparkSession):\\n    # Your code here\\n    pass",
  "solution": "def solution(spark: SparkSession):\\n    # Reference solution...",
  "testCases": "assert result.count() == expected_count",
  "hints": [
    "Hint 1: Start with...",
    "Hint 2: Consider using..."
  ]
}`}</code>
            </pre>
          </Card>
        </section>

        {/* CTA */}
        <section className="text-center">
          <Card className="border-primary/30 bg-gradient-to-br from-primary/5 to-accent/5">
            <CardContent className="py-12">
              <h3 className="text-2xl font-bold mb-4">Ready to Contribute?</h3>
              <p className="text-muted-foreground mb-6 max-w-lg mx-auto">
                Join our community of contributors and help data engineers around the world 
                master PySpark.
              </p>
              <Button size="lg" asChild>
                <a href="https://github.com/ashwin975/pyspark-problem-solver" target="_blank" rel="noopener noreferrer" className="gap-2">
                  <Github className="h-5 w-5" />
                  Get Started on GitHub
                </a>
              </Button>
            </CardContent>
          </Card>
        </section>
      </main>
    </div>
  );
};

export default Contribute;
