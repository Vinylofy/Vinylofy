import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";

function SkeletonBlock({ className = "" }: { className?: string }) {
  return <div className={`animate-pulse rounded-2xl bg-[rgba(63,38,22,0.06)] ${className}`} />;
}

export default function ProductLoadingPage() {
  return (
    <div className="min-h-screen bg-[#f8f7f4] text-neutral-900">
      <SiteHeader />

      <main className="mx-auto max-w-7xl px-6 py-8 md:py-10">
        <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_360px]">
          <section className="space-y-6">
            <div className="rounded-[28px] border border-[rgba(230,126,34,0.16)] bg-white p-5 shadow-sm md:p-7">
              <div className="grid gap-6 md:grid-cols-[188px_minmax(0,1fr)]">
                <SkeletonBlock className="h-[188px] w-[188px]" />
                <div className="space-y-4">
                  <SkeletonBlock className="h-4 w-32" />
                  <SkeletonBlock className="h-10 w-3/4" />
                  <SkeletonBlock className="h-5 w-1/2" />
                  <div className="grid gap-3 md:grid-cols-3">
                    <SkeletonBlock className="h-28 w-full" />
                    <SkeletonBlock className="h-28 w-full" />
                    <SkeletonBlock className="h-28 w-full" />
                  </div>
                </div>
              </div>
            </div>

            <div className="rounded-[28px] border border-[rgba(230,126,34,0.16)] bg-white p-5 shadow-sm md:p-6">
              <SkeletonBlock className="h-7 w-48" />
              <SkeletonBlock className="mt-2 h-4 w-72" />
              <div className="mt-5 space-y-3">
                <SkeletonBlock className="h-24 w-full" />
                <SkeletonBlock className="h-24 w-full" />
                <SkeletonBlock className="h-24 w-full" />
              </div>
            </div>

            <div className="rounded-[28px] border border-[rgba(230,126,34,0.16)] bg-white p-5 shadow-sm md:p-6">
              <SkeletonBlock className="h-7 w-52" />
              <SkeletonBlock className="mt-2 h-4 w-80" />
              <SkeletonBlock className="mt-5 h-[320px] w-full" />
              <div className="mt-6 grid gap-3 md:grid-cols-3">
                <SkeletonBlock className="h-28 w-full" />
                <SkeletonBlock className="h-28 w-full" />
                <SkeletonBlock className="h-28 w-full" />
              </div>
            </div>
          </section>

          <aside className="space-y-6">
            <div className="rounded-[28px] border border-[rgba(230,126,34,0.16)] bg-white p-5 shadow-sm md:p-6">
              <SkeletonBlock className="h-7 w-40" />
              <SkeletonBlock className="mt-4 h-4 w-full" />
              <SkeletonBlock className="mt-2 h-4 w-5/6" />
              <SkeletonBlock className="mt-2 h-4 w-4/5" />
            </div>
          </aside>
        </div>
      </main>

      <SiteFooter />
    </div>
  );
}
