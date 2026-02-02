import { Link } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { Flame, Github, User } from "lucide-react";

const Header = () => {
  return (
    <header className="sticky top-0 z-50 w-full border-b border-border bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="container flex h-14 items-center justify-between">
        <Link to="/" className="flex items-center gap-2">
          <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-primary">
            <Flame className="h-5 w-5 text-primary-foreground" />
          </div>
          <span className="text-xl font-bold tracking-tight">
            Spark<span className="text-primary">Lab</span>
          </span>
        </Link>

        <nav className="flex items-center gap-6">
          <Link 
            to="/problems" 
            className="text-sm font-medium text-muted-foreground transition-colors hover:text-foreground"
          >
            Problems
          </Link>
          <Link 
            to="/learn" 
            className="text-sm font-medium text-muted-foreground transition-colors hover:text-foreground"
          >
            Learn
          </Link>
          <Link 
            to="/contribute" 
            className="text-sm font-medium text-muted-foreground transition-colors hover:text-foreground"
          >
            Contribute
          </Link>
        </nav>

        <div className="flex items-center gap-2">
          <Button variant="ghost" size="icon" asChild>
            <a 
              href="https://github.com" 
              target="_blank" 
              rel="noopener noreferrer"
              aria-label="GitHub"
            >
              <Github className="h-5 w-5" />
            </a>
          </Button>
          <Button variant="outline" size="sm">
            <User className="mr-2 h-4 w-4" />
            Sign In
          </Button>
        </div>
      </div>
    </header>
  );
};

export default Header;
