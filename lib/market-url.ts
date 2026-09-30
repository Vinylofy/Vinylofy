export function marketHref(href: string, marketCode: string): string {
  const [path, query = ""] = href.split("?", 2);
  const params = new URLSearchParams(query);
  params.set("market", marketCode);
  return `${path}?${params.toString()}`;
}
