import { cn } from "@/lib/utils";
import type { Difficulty } from "@/data/problems";

interface DifficultyBadgeProps {
  difficulty: Difficulty;
  className?: string;
}

const DifficultyBadge = ({ difficulty, className }: DifficultyBadgeProps) => {
  const colorMap: Record<Difficulty, string> = {
    Easy: "bg-easy/20 text-easy border-easy/30",
    Medium: "bg-medium/20 text-medium border-medium/30", 
    Hard: "bg-hard/20 text-hard border-hard/30",
  };

  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold",
        colorMap[difficulty],
        className
      )}
    >
      {difficulty}
    </span>
  );
};

export default DifficultyBadge;
