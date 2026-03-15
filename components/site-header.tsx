import { Logo } from "@/components/logo";

export function SiteHeader() {
  return (
    <header className="border-b border-neutral-200 bg-white">
      <div className="mx-auto flex max-w-5xl items-center justify-between px-6 py-4">
        <Logo size="sm" />
      </div>
    </header>
  );
}