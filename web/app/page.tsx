import type { Metadata } from "next";
import { RadarShell } from "./components/RadarShell";

export const metadata: Metadata = {
  title: "SIGNAL//AI · 热点研判台",
  description: "以讨论、行为、来源多样性与证据覆盖判断 AI 行业真实热点。",
};

export default function Home() {
  return <RadarShell />;
}
