export function GlobalSearchBar() {
  return (
    <form action="/search" method="get" className="w-full">
      <div className="flex items-center rounded-full border border-neutral-300 bg-white px-3 py-2 shadow-sm">
        <input
          type="text"
          name="q"
          placeholder="Zoek op artiest, albumtitel of EAN"
          className="w-full bg-transparent px-3 py-2 text-sm outline-none placeholder:text-neutral-400"
        />
        <button
          type="submit"
          className="rounded-full bg-orange-600 px-5 py-2 text-sm font-medium text-white transition hover:bg-orange-700"
        >
          Zoek
        </button>
      </div>
    </form>
  );
}