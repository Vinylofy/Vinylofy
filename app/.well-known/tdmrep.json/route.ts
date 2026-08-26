export const dynamic = "force-static";

const TDM_RESERVATION = [
  {
    location: "/",
    "tdm-reservation": 1,
  },
];

export function GET() {
  return new Response(JSON.stringify(TDM_RESERVATION), {
    headers: {
      "content-type": "application/json; charset=utf-8",
    },
  });
}
