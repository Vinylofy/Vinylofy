import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";

function SkeletonBlock({ className = "" }: { className?: string }) {
  return <div className={`animate-pulse rounded-xl bg-[rgba(63,38,22,0.06)] ${className}`} />;
}

export default function ProductLoadingPage() {
  return (
    <div className="min-h-screen bg-[#f8f7f4] text-neutral-900">
      <SiteHeader />

      <main className="mx-auto max-w-6xl px-4 py-6 md:px-6 md:py-8">
        <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_300px] xl:gap-5">
          <section className="space-y-4 md:space-y-5">
            <div className="rounded-xl border border-[rgba(230,126,34,0.16)] bg-white p-4 shadow-sm md:p-5">
              <div className="grid gap-4 md:grid-cols-[132px_minmax(0,1fr)]">
                <SkeletonBlock className="h-[132px] w-[132px]" />
                <div className="space-y-3">
                  <SkeletonBlock className="h-3.5 w-24" />
                  <SkeletonBlock className="h-8 w-3/4" />
                  <SkeletonBlock className="h-4 w-1/2" />
                  <div className="grid gap-2 md:grid-cols-3">
                    <SkeletonBlock className="h-20 w-full" />
                    <SkeletonBlock className="h-20 w-full" />
                    <SkeletonBlock className="h-20 w-full" />
                  </div>
                </div>
              </div>
            </div>

            <div className="rounded-xl border border-[rgba(230,126,34,0.16)] bg-white p-4 shadow-sm md:p-5">
              <SkeletonBlock className="h-6 w-36" />
              <SkeletonBlock className="mt-2 h-3.5 w-56" />
              <div className="mt-4 space-y-2.5">
                <SkeletonBlock className="h-16 w-full" />
                <SkeletonBlock className="h-16 w-full" />
                <SkeletonBlock className="h-16 w-full" />
              </div>
            </div>

            <div className="rounded-xl border border-[rgba(230,126,34,0.16)] bg-white p-4 shadow-sm md:p-5">
              <SkeletonBlock className="h-6 w-40" />
              <SkeletonBlock className="mt-2 h-3.5 w-72" />
              <SkeletonBlock className="mt-4 h-[210px] w-full" />
              <div className="mt-4 grid gap-2 md:grid-cols-3">
                <SkeletonBlock className="h-16 w-full" />
                <SkeletonBlock className="h-16 w-full" />
                <SkeletonBlock className="h-16 w-full" />
              </div>
            </div>
          </section>

          <aside className="space-y-4">
            <div className="rounded-xl border border-[rgba(230,126,34,0.16)] bg-white p-4 shadow-sm md:p-5">
              <SkeletonBlock className="h-6 w-32" />
              <SkeletonBlock className="mt-3 h-3.5 w-full" />
              <SkeletonBlock className="mt-2 h-3.5 w-5/6" />
              <SkeletonBlock className="mt-2 h-3.5 w-4/5" />
            </div>
          </aside>
        </div>
      </main>

      <SiteFooter />
    </div>
  );
}
