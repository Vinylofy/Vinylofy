import { cn } from "@/lib/utils";

type LogoProps = {
  size?: "sm" | "md" | "lg";
  withTagline?: boolean;
};

export function Logo({ size = "md", withTagline = false }: LogoProps) {
  return (
    <div className="inline-flex flex-col">
      <div
        className={cn(
          "inline-flex items-center font-semibold tracking-tight",
          size === "sm" && "text-xl",
          size === "md" && "text-2xl",
          size === "lg" && "text-4xl md:text-5xl"
        )}
        aria-label="Vinylofy"
      >
        <span className="text-neutral-950">Vinyl</span>
        <span className="text-orange-600">ofy</span>
      </div>

      {withTagline ? (
        <span className="mt-1 text-xs uppercase tracking-[0.16em] text-neutral-400 md:text-sm">
          Vinyl offers for you
        </span>
      ) : null}
    </div>
  );
}