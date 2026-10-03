import posthog from "posthog-js";

const token = process.env.NEXT_PUBLIC_POSTHOG_PROJECT_TOKEN;
const host = process.env.NEXT_PUBLIC_POSTHOG_HOST;

if (token && host && !posthog.__loaded) {
  posthog.init(token, {
    api_host: host,
    defaults: "2026-05-30",
    capture_pageview: "history_change",
    capture_performance: true,
    autocapture: false,
    session_recording: {
      maskAllInputs: true,
    },
  });
}
