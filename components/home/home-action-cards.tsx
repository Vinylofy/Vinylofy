import Image from "next/image";
import Link from "next/link";

type QuickLink = {
  href: string;
  label: string;
  imageSrc: string;
};

const links: QuickLink[] = [
  {
    href: "/nieuwe-releases",
    label: "Nieuwe releases",
    imageSrc: "/home-cards/nieuwe-releases.png",
  },
  {
    href: "/top-25",
    label: "Populaire vinyl",
    imageSrc: "/home-cards/top-25.png",
  },
  {
    href: "/topdeals",
    label: "BestOffers",
    imageSrc: "/home-cards/best-deals.png",
  },
];

export function HomeActionCards() {
  return (
    <nav
      aria-label="Snelle navigatie"
      className="mx-auto grid w-full max-w-[920px] grid-cols-3 gap-4 md:gap-8"
    >
      {links.map(({ href, label, imageSrc }) => (
        <Link
          key={href}
          href={href}
          className="group flex flex-col items-center justify-start text-center"
        >
          <div className="flex h-[88px] items-center justify-center md:h-[96px]">
            <Image
              src={imageSrc}
              alt=""
              width={256}
              height={256}
              className="h-full w-auto object-contain transition group-hover:scale-[1.02]"
            />
          </div>

          <span className="mt-2 text-sm font-medium text-neutral-700 transition group-hover:text-orange-600 md:text-base">
            {label}
          </span>
        </Link>
      ))}
    </nav>
  );
}
