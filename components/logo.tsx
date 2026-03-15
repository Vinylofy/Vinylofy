import clsx from "clsx";

type LogoProps = {
  size?: "sm" | "md" | "lg";
};

export function Logo({ size = "md" }: LogoProps) {
  return (
    <div
      className={clsx(
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
  );
}