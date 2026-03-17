type GlobalSearchBarProps = {
  defaultValue?: string;
  compact?: boolean;
};

export function GlobalSearchBar({
  defaultValue = "",
  compact = false,
}: GlobalSearchBarProps) {
  return (
    <form action="/search" method="get" className="w-full">
      <div
        className={[
          "flex items-center rounded-full border border-neutral-300 bg-white shadow-sm",
          compact ? "px-2 py-2" : "px-3 py-2",
        ].join(" ")}
      >
        <input
          type="text"
          name="q"
          defaultValue={defaultValue}
          placeholder="Zoek op artiest of albumtitel"
          className={[
            "w-full bg-transparent outline-none placeholder:text-neutral-400",
            compact ? "px-3 py-1 text-sm" : "px-3 py-2 text-sm md:text-base",
          ].join(" ")}
        />
        <button
          type="submit"
          className={[
            "rounded-full bg-orange-600 font-medium text-white transition hover:bg-orange-700",
            compact ? "px-4 py-2 text-sm" : "px-5 py-2 text-sm",
          ].join(" ")}
        >
          Zoek
        </button>
      </div>
    </form>
  );
}