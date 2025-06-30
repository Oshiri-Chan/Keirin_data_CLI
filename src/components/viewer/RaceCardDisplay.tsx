"use client";

import React, { useState, useEffect } from 'react';

// 仮のデータ型定義
interface PlayerEntry {
  waku: number; // 枠番
  carNumber: number; // 車番
  name: string;
  legType: string; // 脚質 (例: 逃, 追, 両)
  comment: string;
  playerId: string; // 選手を一意に識別するID
}

interface RaceCardData {
  raceName: string;
  entries: PlayerEntry[];
}

interface RaceCardDisplayProps {
  selectedDate: string | null;
  selectedVenueId: string | null;
  selectedRaceId: string | null;
  onPlayerSelect: (playerId: string) => void; // 親に選手選択を通知する関数
}

const RaceCardDisplay: React.FC<RaceCardDisplayProps> = ({
  selectedDate,
  selectedVenueId,
  selectedRaceId,
  onPlayerSelect,
}) => {
  const [raceCardData, setRaceCardData] = useState<RaceCardData | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // 日付、会場、レースがすべて選択されたらデータを取得
    if (selectedDate && selectedVenueId && selectedRaceId) {
      setLoading(true);
      setError(null);
      setRaceCardData(null); // 表示をクリア

      // TODO: Replace with actual API call
      // fetch(`/api/race-card?date=${selectedDate}&venue=${selectedVenueId}&race=${selectedRaceId}`)
      console.log(`Fetching race card for: ${selectedDate}, ${selectedVenueId}, ${selectedRaceId}`);
      // Simulate API call
      const fetchRaceCard = async () => {
        try {
           // ここで実際のAPIを叩く
           // const response = await fetch(`/api/race-card?date=${selectedDate}&venue=${selectedVenueId}&race=${selectedRaceId}`);
           // if (!response.ok) throw new Error('Failed to fetch race card');
           // const data: RaceCardData = await response.json();

           // Simulate API fetch delay and data
           await new Promise(resolve => setTimeout(resolve, 700));
           const mockData: RaceCardData = {
             raceName: `${selectedRaceId}R ${selectedVenueId === 'track_a' ? '特選' : '決勝'}`, // Example name
             entries: [
               { waku: 1, carNumber: 1, name: `Player A (R${selectedRaceId})`, legType: '逃', comment: '先行意欲', playerId: `p1_${selectedRaceId}` },
               { waku: 2, carNumber: 2, name: `Player B (R${selectedRaceId})`, legType: '追', comment: '自在に', playerId: `p2_${selectedRaceId}` },
               { waku: 3, carNumber: 3, name: `Player C (R${selectedRaceId})`, legType: '両', comment: '好位から', playerId: `p3_${selectedRaceId}` },
               { waku: 4, carNumber: 4, name: `Player D (R${selectedRaceId})`, legType: '逃', comment: '積極的に', playerId: `p4_${selectedRaceId}` },
               { waku: 5, carNumber: 5, name: `Player E (R${selectedRaceId})`, legType: '追', comment: 'マーク屋', playerId: `p5_${selectedRaceId}` },
               // Add more players based on raceId if needed
             ]
           };
           setRaceCardData(mockData);

        } catch (err) {
          setError(err instanceof Error ? err.message : 'An unknown error occurred');
          console.error("Fetch error:", err);
        } finally {
          setLoading(false);
        }
      };

      fetchRaceCard();

    } else {
      // 何も選択されていない場合は表示をクリア
      setRaceCardData(null);
      setLoading(false);
      setError(null);
    }
  }, [selectedDate, selectedVenueId, selectedRaceId]); // Propsの変更を検知

  // --- レンダリング ---
  if (!selectedDate || !selectedVenueId || !selectedRaceId) {
    return (
      <div className="bg-white p-6 rounded shadow-md text-gray-500">
        Please select a date, venue, and race to view the card.
      </div>
    );
  }

  if (loading) {
    return (
      <div className="bg-white p-6 rounded shadow-md text-center">
        <p className="text-lg font-semibold animate-pulse text-blue-600">Loading Race Card...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white p-6 rounded shadow-md text-red-600">
        <p>Error loading race card: {error}</p>
      </div>
    );
  }

  if (!raceCardData) {
    return (
      <div className="bg-white p-6 rounded shadow-md text-gray-500">
        No race card data available for the selection.
      </div>
    );
  }

  return (
    <div className="bg-white p-6 rounded shadow-md">
      <h2 className="text-2xl font-bold mb-4 text-gray-800">
        {raceCardData.raceName} - 出走表
      </h2>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">枠</th>
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">車番</th>
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">選手名</th>
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">脚質</th>
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">コメント</th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {raceCardData.entries.map((player) => (
              <tr
                key={player.playerId}
                className="hover:bg-gray-100 cursor-pointer"
                onClick={() => onPlayerSelect(player.playerId)} // クリックで選手IDを通知
              >
                <td className="px-4 py-2 whitespace-nowrap text-sm font-medium text-gray-900">{player.waku}</td>
                <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-700">{player.carNumber}</td>
                <td className="px-4 py-2 whitespace-nowrap text-sm font-semibold text-blue-700">{player.name}</td>
                <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-600">{player.legType}</td>
                <td className="px-4 py-2 text-sm text-gray-500">{player.comment}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default RaceCardDisplay; 