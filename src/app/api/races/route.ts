import { NextRequest, NextResponse } from 'next/server';

interface Race {
  id: string;
  name: string;
}

// ダミーデータ
const raceData: { [key: string]: { [key: string]: Race[] } } = {
  '2023-10-27': { // Example Date - Replace with current logic if needed
    'track_a': [
      { id: '1', name: '1R 予選' }, { id: '2', name: '2R 一般' }, { id: '11', name: '11R 特選' }, { id: '12', name: '12R 決勝' }
    ],
    'track_b': [
      { id: '1', name: '1R ガールズ予選' }, { id: '7', name: '7R A級特選' }, { id: '10', name: '10R S級決勝' }
    ],
  },
  '2023-10-28': { // Example Date + 1 - Replace with current logic if needed
     'track_a': [
      { id: '3', name: '3R A級予選' }, { id: '8', name: '8R S級予選' }, { id: '12', name: '12R 選抜' }
    ],
    // Track B has no races on this mock date
  }
  // Add more dates and venues dynamically based on current date
};

// Helper function to get today and tomorrow's date strings
const getRelevantDates = () => {
  const today = new Date();
  const tomorrow = new Date();
  tomorrow.setDate(today.getDate() + 1);
  const formatDate = (date: Date) => date.toISOString().split('T')[0];
  return { todayStr: formatDate(today), tomorrowStr: formatDate(tomorrow) };
}

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const date = searchParams.get('date');
  const venue = searchParams.get('venue');
  const { todayStr, tomorrowStr } = getRelevantDates();

  // Simple validation
  if (!date || !venue) {
    return NextResponse.json({ error: 'Date and venue parameters are required' }, { status: 400 });
  }

  // --- More dynamic mock data generation ---
  // Use current date to generate predictable mock data if static keys don't match
  let racesForDate = raceData[date];

  if (!racesForDate && (date === todayStr || date === tomorrowStr)) {
    // Generate predictable mock data for today/tomorrow if not hardcoded
    racesForDate = {};
    if (venue === 'track_a') {
      racesForDate[venue] = [
          { id: '1', name: '1R 予選 (Dynamic)' },
          { id: '5', name: '5R 一般 (Dynamic)' },
          { id: '12', name: '12R 決勝 (Dynamic)' },
      ];
    } else if (venue === 'track_b') {
        racesForDate[venue] = [
          { id: '2', name: '2R ガールズ (Dynamic)' },
          { id: '9', name: '9R 特選 (Dynamic)' },
      ];
    }
    // Add other venues or logic as needed
  }
  // --- End dynamic mock data ---


  const races = racesForDate ? (racesForDate[venue] || []) : [];

  // Simulate network delay
  await new Promise(resolve => setTimeout(resolve, 300));

  return NextResponse.json(races);
} 