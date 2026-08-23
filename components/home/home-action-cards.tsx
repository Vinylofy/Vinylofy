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
    imageSrc: "/home-cards/blok1.png",
  },
  {
    href: "/top-33",
    label: "Top 33",
    imageSrc: "/home-cards/blok2.png",
  },
  {
    href: "/topdeals",
    label: "Best Deals",
    imageSrc: "/home-cards/blok3.png",
  },
  {
    href: "/follow-the-groove",
    label: "Follow the Groove",
    imageSrc: "/home-cards/blok4.png",
  },
];

export function HomeActionCards() {
  return (
    <nav
      aria-label="Snelle navigatie"
      className="mx-auto grid w-full max-w-[920px] grid-cols-2 gap-4 md:grid-cols-4 md:gap-8"
    >
      {links.map(({ href, label, imageSrc }) => (
        <Link
          key={href}
          href={href}
          aria-label={label}
          className="group flex flex-col items-center justify-start text-center"
        >
          <div className="flex h-[148px] items-center justify-center md:h-[164px]">
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
