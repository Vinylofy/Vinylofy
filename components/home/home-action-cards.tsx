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
    imageSrc: "/home-cards/nieuwe-releases-white.png",
  },
  {
    href: "/top-25",
    label: "Top 25",
    imageSrc: "/home-cards/top-25-white.png",
  },
  {
    href: "/topdeals",
    label: "Best Deals",
    imageSrc: "/home-cards/best-deals-white.png",
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
          aria-label={label}
          className="group flex flex-col items-center justify-start text-center"
        >
          <div className="flex h-[128px] items-center justify-center md:h-[144px]">
            <Image
              src={imageSrc}
              alt=""
              width={360}
              height={360}
              className="h-full w-auto object-contain transition group-hover:scale-[1.02]"
            />
          </div>
        </Link>
      ))}
    </nav>
  );
}
