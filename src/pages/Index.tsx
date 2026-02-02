import { Link } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import Header from "@/components/Header";
import { 
  Flame, 
  Code2, 
  Database, 
  Zap, 
  ArrowRight, 
  Github,
  BookOpen,
  Users,
  Trophy
} from "lucide-react";
import { problems, categories } from "@/data/problems";

const Index = () => {
  const stats = {
    problems: problems.length,
    categories: categories.length,
    contributors: 52, // ZillaCode inspired - now with 52 problems!
  };

  return (
    <div className="min-h-screen bg-background">
      <Header />
      
      {/* Hero Section */}
      <section className="relative overflow-hidden border-b border-border">
        <div className="absolute inset-0 bg-gradient-to-br from-primary/5 via-transparent to-accent/5" />
        <div className="container relative py-24 md:py-32">
          <div className="mx-auto max-w-3xl text-center">
            <div className="mb-6 inline-flex items-center gap-2 rounded-full border border-primary/30 bg-primary/10 px-4 py-1.5 text-sm font-medium text-primary">
              <Flame className="h-4 w-4" />
              Master PySpark through Practice
            </div>
            
            <h1 className="mb-6 text-4xl font-bold tracking-tight sm:text-5xl md:text-6xl">
              Level Up Your{" "}
              <span className="text-primary">PySpark</span>{" "}
              Skills
            </h1>
            
            <p className="mb-8 text-lg text-muted-foreground md:text-xl">
              Practice with hands-on coding challenges designed for data engineers. 
              From DataFrame basics to advanced window functions, build real-world skills.
            </p>
            
            <div className="flex flex-col items-center justify-center gap-4 sm:flex-row">
              <Button size="lg" asChild className="gap-2">
                <Link to="/problems">
                  Start Practicing
                  <ArrowRight className="h-4 w-4" />
                </Link>
              </Button>
              <Button size="lg" variant="outline" asChild className="gap-2">
                <a href="https://github.com" target="_blank" rel="noopener noreferrer">
                  <Github className="h-4 w-4" />
                  Contribute
                </a>
              </Button>
            </div>
          </div>
        </div>
      </section>

      {/* Stats Section */}
      <section className="border-b border-border bg-secondary/30">
        <div className="container py-12">
          <div className="grid grid-cols-3 gap-8">
            <div className="text-center">
              <div className="text-3xl font-bold text-primary">{stats.problems}+</div>
              <div className="text-sm text-muted-foreground">Practice Problems</div>
            </div>
            <div className="text-center">
              <div className="text-3xl font-bold text-primary">{stats.categories}</div>
              <div className="text-sm text-muted-foreground">Categories</div>
            </div>
            <div className="text-center">
              <div className="text-3xl font-bold text-primary">{stats.contributors}+</div>
              <div className="text-sm text-muted-foreground">Contributors</div>
            </div>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <section className="py-20">
        <div className="container">
          <div className="mx-auto mb-12 max-w-2xl text-center">
            <h2 className="mb-4 text-3xl font-bold">Built for Data Engineers</h2>
            <p className="text-muted-foreground">
              Everything you need to master PySpark and advance your data engineering career.
            </p>
          </div>
          
          <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
            <Card className="border-border bg-card transition-all hover:border-primary/30 hover:shadow-lg hover:shadow-primary/5">
              <CardContent className="pt-6">
                <div className="mb-4 flex h-12 w-12 items-center justify-center rounded-lg bg-primary/10">
                  <Code2 className="h-6 w-6 text-primary" />
                </div>
                <h3 className="mb-2 text-lg font-semibold">Interactive Editor</h3>
                <p className="text-sm text-muted-foreground">
                  Write and test PySpark code with syntax highlighting and intelligent autocomplete.
                </p>
              </CardContent>
            </Card>
            
            <Card className="border-border bg-card transition-all hover:border-primary/30 hover:shadow-lg hover:shadow-primary/5">
              <CardContent className="pt-6">
                <div className="mb-4 flex h-12 w-12 items-center justify-center rounded-lg bg-accent/10">
                  <Database className="h-6 w-6 text-accent" />
                </div>
                <h3 className="mb-2 text-lg font-semibold">Real Scenarios</h3>
                <p className="text-sm text-muted-foreground">
                  Practice with problems based on actual data engineering challenges.
                </p>
              </CardContent>
            </Card>
            
            <Card className="border-border bg-card transition-all hover:border-primary/30 hover:shadow-lg hover:shadow-primary/5">
              <CardContent className="pt-6">
                <div className="mb-4 flex h-12 w-12 items-center justify-center rounded-lg bg-info/10">
                  <Zap className="h-6 w-6 text-info" />
                </div>
                <h3 className="mb-2 text-lg font-semibold">Instant Feedback</h3>
                <p className="text-sm text-muted-foreground">
                  Get immediate validation of your solutions with detailed test results.
                </p>
              </CardContent>
            </Card>

            <Card className="border-border bg-card transition-all hover:border-primary/30 hover:shadow-lg hover:shadow-primary/5">
              <CardContent className="pt-6">
                <div className="mb-4 flex h-12 w-12 items-center justify-center rounded-lg bg-success/10">
                  <BookOpen className="h-6 w-6 text-success" />
                </div>
                <h3 className="mb-2 text-lg font-semibold">Learning Resources</h3>
                <p className="text-sm text-muted-foreground">
                  Access hints, explanations, and optimized solutions for every problem.
                </p>
              </CardContent>
            </Card>

            <Card className="border-border bg-card transition-all hover:border-primary/30 hover:shadow-lg hover:shadow-primary/5">
              <CardContent className="pt-6">
                <div className="mb-4 flex h-12 w-12 items-center justify-center rounded-lg bg-warning/10">
                  <Users className="h-6 w-6 text-warning" />
                </div>
                <h3 className="mb-2 text-lg font-semibold">Community Driven</h3>
                <p className="text-sm text-muted-foreground">
                  Contribute problems and solutions via GitHub pull requests.
                </p>
              </CardContent>
            </Card>

            <Card className="border-border bg-card transition-all hover:border-primary/30 hover:shadow-lg hover:shadow-primary/5">
              <CardContent className="pt-6">
                <div className="mb-4 flex h-12 w-12 items-center justify-center rounded-lg bg-hard/10">
                  <Trophy className="h-6 w-6 text-hard" />
                </div>
                <h3 className="mb-2 text-lg font-semibold">Track Progress</h3>
                <p className="text-sm text-muted-foreground">
                  Monitor your progress and celebrate milestones as you improve.
                </p>
              </CardContent>
            </Card>
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="border-t border-border bg-gradient-to-br from-primary/5 to-accent/5 py-20">
        <div className="container">
          <div className="mx-auto max-w-2xl text-center">
            <h2 className="mb-4 text-3xl font-bold">Ready to Start?</h2>
            <p className="mb-8 text-muted-foreground">
              Jump into your first problem and start building real PySpark skills today.
            </p>
            <Button size="lg" asChild>
              <Link to="/problems" className="gap-2">
                Browse Problems
                <ArrowRight className="h-4 w-4" />
              </Link>
            </Button>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="border-t border-border py-8">
        <div className="container flex flex-col items-center justify-between gap-4 sm:flex-row">
          <div className="flex items-center gap-2">
            <Flame className="h-5 w-5 text-primary" />
            <span className="font-semibold">SparkLab</span>
          </div>
          <p className="text-sm text-muted-foreground">
            Open source PySpark learning platform. Contribute on{" "}
            <a href="https://github.com" className="text-primary hover:underline">
              GitHub
            </a>
            .
          </p>
        </div>
      </footer>
    </div>
  );
};

export default Index;
