import { Link } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { CheckCircle2, Circle } from "lucide-react";
import DifficultyBadge from "./DifficultyBadge";
import type { Problem } from "@/data/problems";
import { cn } from "@/lib/utils";

interface ProblemCardProps {
  problem: Problem;
  index: number;
  isSolved?: boolean;
}

const ProblemCard = ({ problem, index, isSolved = false }: ProblemCardProps) => {
  return (
    <Link to={`/problem/${problem.id}`}>
      <Card className="group cursor-pointer transition-all duration-200 hover:border-primary/50 hover:shadow-lg hover:shadow-primary/5">
        <CardHeader className="pb-3">
          <div className="flex items-start justify-between gap-4">
            <div className="flex items-center gap-3">
              <span className="flex h-8 w-8 items-center justify-center rounded-full bg-secondary text-sm font-medium text-muted-foreground">
                {index + 1}
              </span>
              <div>
                <CardTitle className="text-base font-semibold group-hover:text-primary transition-colors">
                  {problem.title}
                </CardTitle>
                <p className="text-xs text-muted-foreground mt-1">
                  {problem.category}
                </p>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <DifficultyBadge difficulty={problem.difficulty} />
              {isSolved ? (
                <CheckCircle2 className="h-5 w-5 text-success" />
              ) : (
                <Circle className="h-5 w-5 text-muted-foreground/30" />
              )}
            </div>
          </div>
        </CardHeader>
        <CardContent className="pt-0">
          <p className="text-sm text-muted-foreground line-clamp-2">
            {problem.description.split('\n')[0]}
          </p>
        </CardContent>
      </Card>
    </Link>
  );
};

export default ProblemCard;
