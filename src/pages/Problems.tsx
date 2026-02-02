import { useState } from "react";
import Header from "@/components/Header";
import ProblemCard from "@/components/ProblemCard";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { problems, categories, type Difficulty, type Category } from "@/data/problems";
import { Search, Filter, X } from "lucide-react";
import { cn } from "@/lib/utils";

const Problems = () => {
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedDifficulty, setSelectedDifficulty] = useState<Difficulty | null>(null);
  const [selectedCategory, setSelectedCategory] = useState<Category | null>(null);

  const difficulties: Difficulty[] = ["Easy", "Medium", "Hard"];

  const filteredProblems = problems.filter((problem) => {
    const matchesSearch = problem.title.toLowerCase().includes(searchQuery.toLowerCase()) ||
                         problem.description.toLowerCase().includes(searchQuery.toLowerCase());
    const matchesDifficulty = !selectedDifficulty || problem.difficulty === selectedDifficulty;
    const matchesCategory = !selectedCategory || problem.category === selectedCategory;
    
    return matchesSearch && matchesDifficulty && matchesCategory;
  });

  const clearFilters = () => {
    setSearchQuery("");
    setSelectedDifficulty(null);
    setSelectedCategory(null);
  };

  const hasActiveFilters = searchQuery || selectedDifficulty || selectedCategory;

  return (
    <div className="min-h-screen bg-background">
      <Header />
      
      <main className="container py-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold mb-2">Practice Problems</h1>
          <p className="text-muted-foreground">
            {problems.length} problems to help you master PySpark
          </p>
        </div>

        {/* Filters */}
        <div className="mb-6 space-y-4">
          {/* Search */}
          <div className="relative max-w-md">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="Search problems..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10"
            />
          </div>

          {/* Difficulty Filter */}
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-sm font-medium text-muted-foreground mr-2">Difficulty:</span>
            {difficulties.map((difficulty) => (
              <Button
                key={difficulty}
                variant={selectedDifficulty === difficulty ? "default" : "outline"}
                size="sm"
                onClick={() => setSelectedDifficulty(
                  selectedDifficulty === difficulty ? null : difficulty
                )}
                className={cn(
                  "text-xs",
                  selectedDifficulty === difficulty && difficulty === "Easy" && "bg-easy hover:bg-easy/90",
                  selectedDifficulty === difficulty && difficulty === "Medium" && "bg-medium hover:bg-medium/90",
                  selectedDifficulty === difficulty && difficulty === "Hard" && "bg-hard hover:bg-hard/90"
                )}
              >
                {difficulty}
              </Button>
            ))}
          </div>

          {/* Category Filter */}
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-sm font-medium text-muted-foreground mr-2">Category:</span>
            {categories.map((category) => (
              <Badge
                key={category}
                variant={selectedCategory === category ? "default" : "outline"}
                className={cn(
                  "cursor-pointer transition-colors",
                  selectedCategory === category 
                    ? "bg-primary text-primary-foreground" 
                    : "hover:bg-secondary"
                )}
                onClick={() => setSelectedCategory(
                  selectedCategory === category ? null : category
                )}
              >
                {category}
              </Badge>
            ))}
          </div>

          {/* Clear Filters */}
          {hasActiveFilters && (
            <Button
              variant="ghost"
              size="sm"
              onClick={clearFilters}
              className="text-muted-foreground hover:text-foreground"
            >
              <X className="mr-2 h-4 w-4" />
              Clear filters
            </Button>
          )}
        </div>

        {/* Results */}
        <div className="mb-4">
          <p className="text-sm text-muted-foreground">
            Showing {filteredProblems.length} of {problems.length} problems
          </p>
        </div>

        {/* Problem Grid */}
        {filteredProblems.length > 0 ? (
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {filteredProblems.map((problem, index) => (
              <ProblemCard 
                key={problem.id} 
                problem={problem} 
                index={problems.indexOf(problem)} 
              />
            ))}
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center py-12 text-center">
            <Filter className="h-12 w-12 text-muted-foreground/50 mb-4" />
            <h3 className="text-lg font-medium mb-2">No problems found</h3>
            <p className="text-sm text-muted-foreground mb-4">
              Try adjusting your search or filters
            </p>
            <Button variant="outline" onClick={clearFilters}>
              Clear all filters
            </Button>
          </div>
        )}
      </main>
    </div>
  );
};

export default Problems;
