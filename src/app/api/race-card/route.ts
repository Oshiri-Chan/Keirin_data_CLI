import { NextRequest, NextResponse } from 'next/server';

interface PlayerEntry {
  waku: number;
  carNumber: number;
  name: string;
  legType: string;
  comment: string;
  playerId: string;
}

interface RaceCardData {
  raceName: string;
  entries: PlayerEntry[];
}

// ダミーデータ生成ロジック (実際にはDBから取得)
const generateMockData = (date: string, venue: string, race: string): RaceCardData | null => {
  // 簡単な例: venueとraceに基づいてデータを生成
  const raceNum = parseInt(race, 10);
  if (isNaN(raceNum)) return null; // Invalid race number

  const entries: PlayerEntry[] = [];
  let numPlayers = 9; // Default to 9 players
  let raceTitleSuffix = '一般';

  if (venue === 'track_a') {
    if (raceNum === 12) { numPlayers = 7; raceTitleSuffix = '決勝'; }
    else if (raceNum >= 10) { raceTitleSuffix = '特選'; }
    else { raceTitleSuffix = '予選'; }
  } else if (venue === 'track_b') {
    numPlayers = 7; // Assume girls keirin
    if (raceNum === 10) { raceTitleSuffix = 'S級決勝'; }
    else if (raceNum >= 7) { raceTitleSuffix = 'A級特選'; }
    else { raceTitleSuffix = 'ガールズ予選'; }
  }

  for (let i = 1; i <= numPlayers; i++) {
    entries.push({
      waku: Math.ceil(i / 2), // Basic waku assignment
      carNumber: i,
      name: `選手 ${String.fromCharCode(64 + i)} (R${race} ${venue.replace('track_','')})`,
      legType: i % 3 === 0 ? '両' : (i % 2 === 0 ? '追' : '逃'),
      comment: `コメント ${i} on ${date}`,
      playerId: `p${i}_${venue}_${race}_${date}`, // Unique ID for player instance in this race
    });
  }

  const venueName = venue === 'track_a' ? 'Track A' : (venue === 'track_b' ? 'Track B' : venue);

  return {
    raceName: `${race}R ${venueName} ${raceTitleSuffix}`,
    entries
  };
}

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const date = searchParams.get('date');
  const venue = searchParams.get('venue');
  const race = searchParams.get('race');

  if (!date || !venue || !race) {
    return NextResponse.json({ error: 'Date, venue, and race parameters are required' }, { status: 400 });
  }

  // Simulate network delay & generate mock data
  await new Promise(resolve => setTimeout(resolve, 500));
  const data = generateMockData(date, venue, race);

  if (!data) {
    return NextResponse.json({ error: 'Race card not found for the selection' }, { status: 404 });
  }

  return NextResponse.json(data);
} 