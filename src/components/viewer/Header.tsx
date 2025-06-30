"use client";

import React, { useState, useEffect, ChangeEvent } from 'react';

// 仮のデータ型
interface Venue {
  id: string;
  name: string;
}
interface Race {
  id: string;
  name: string; // 例: "1R 予選"
}


interface HeaderProps {
  initialDate: string | null;
  onDateChange: (date: string | null) => void;
  onVenueChange: (venueId: string | null) => void;
  onRaceChange: (raceId: string | null) => void;
}


const Header: React.FC<HeaderProps> = ({
  initialDate,
  onDateChange,
  onVenueChange,
  onRaceChange
}) => {
  const today = new Date().toISOString().split('T')[0];
  const tomorrow = new Date(Date.now() + 86400000).toISOString().split('T')[0];

  // Internal state still needed for dropdown options
  const [currentDate, setCurrentDate] = useState<string>(initialDate || today);
  const [currentVenue, setCurrentVenue] = useState<string>('');
  const [currentRace, setCurrentRace] = useState<string>('');


  const [venues, setVenues] = useState<Venue[]>([]);
  const [races, setRaces] = useState<Race[]>([]);

  const [loadingVenues, setLoadingVenues] = useState<boolean>(false);
  const [loadingRaces, setLoadingRaces] = useState<boolean>(false);

  // Fetch Venues when date changes
  useEffect(() => {
    // Ensure initialDate is valid before fetching
    if (currentDate) {
        setLoadingVenues(true);
        // TODO: Replace with actual API: fetch(`/api/venues?date=${currentDate}`)
        console.log(`Fetching venues for date: ${currentDate}`);
        setTimeout(() => {
            const mockVenues: Venue[] = [
                { id: 'track_a', name: 'Track A' },
                { id: 'track_b', name: 'Track B' },
            ];
            setVenues(mockVenues);
            setCurrentVenue(''); // Reset venue selection
            onVenueChange(null); // Notify parent
            setRaces([]);       // Reset race list
            setCurrentRace('');
            onRaceChange(null); // Notify parent
            setLoadingVenues(false);
        }, 500);
    } else {
        setVenues([]);
        setCurrentVenue('');
        onVenueChange(null);
        setRaces([]);
        setCurrentRace('');
        onRaceChange(null);
    }
  }, [currentDate, onVenueChange, onRaceChange]); // Add dependencies

  // Fetch Races when venue changes
  useEffect(() => {
    if (currentDate && currentVenue) {
      setLoadingRaces(true);
      // TODO: Replace with actual API: fetch(`/api/races?date=${currentDate}&venue=${currentVenue}`)
      console.log(`Fetching races for date: ${currentDate}, venue: ${currentVenue}`);
      setTimeout(() => {
        let mockRaces: Race[] = [];
         if (currentVenue === 'track_a') {
          mockRaces = [ { id: '1', name: '1R 予選' }, { id: '11', name: '11R 特選' }, { id: '12', name: '12R 決勝' }];
        } else if (currentVenue === 'track_b') {
           mockRaces = [ { id: '1', name: '1R ガールズ' }, { id: '7', name: '7R A級' }, { id: '10', name: '10R S級' }];
        }
        setRaces(mockRaces);
        setCurrentRace(''); // Reset race selection
        onRaceChange(null); // Notify parent
        setLoadingRaces(false);
      }, 500);
    } else {
      setRaces([]);
      setCurrentRace('');
       onRaceChange(null);
    }
  }, [currentDate, currentVenue, onRaceChange]); // Add dependencies


  // --- Event Handlers ---
  const handleDateChange = (e: ChangeEvent<HTMLSelectElement>) => {
    const newDate = e.target.value;
    setCurrentDate(newDate);
    onDateChange(newDate); // Notify parent
  };

  const handleVenueChange = (e: ChangeEvent<HTMLSelectElement>) => {
    const newVenue = e.target.value;
    setCurrentVenue(newVenue);
    onVenueChange(newVenue || null); // Notify parent (pass null if empty)
  };

   const handleRaceChange = (e: ChangeEvent<HTMLSelectElement>) => {
    const newRace = e.target.value;
    setCurrentRace(newRace);
    onRaceChange(newRace || null); // Notify parent (pass null if empty)
     console.log(`Selected Race ID: ${newRace}`);
  };

  // Render logic remains similar, using internal state (currentDate, currentVenue, etc.)
  // for dropdown values and options, and disabling based on loading state.
  return (
     <header className="bg-gray-800 text-white p-4 shadow-md">
      <div className="container mx-auto flex flex-wrap items-center justify-start gap-4">
        {/* Date Selector */}
        <div className="flex items-center gap-2">
          <label htmlFor="date-select" className="font-medium">Date:</label>
          <select
            id="date-select"
            value={currentDate} // Use internal state for value
            onChange={handleDateChange}
            className="bg-gray-700 border border-gray-600 rounded px-2 py-1 text-white focus:outline-none focus:border-blue-500"
          >
            <option value={today}>{today} (Today)</option>
            <option value={tomorrow}>{tomorrow} (Tomorrow)</option>
          </select>
        </div>

        {/* Venue Selector */}
        <div className="flex items-center gap-2">
          <label htmlFor="venue-select" className="font-medium">Venue:</label>
          <select
            id="venue-select"
            value={currentVenue} // Use internal state
            onChange={handleVenueChange}
            disabled={loadingVenues || venues.length === 0}
            className="bg-gray-700 border border-gray-600 rounded px-2 py-1 text-white focus:outline-none focus:border-blue-500 disabled:opacity-50"
          >
            <option value="">{loadingVenues ? 'Loading...' : 'Select Venue...'}</option>
            {venues.map((venue) => (
              <option key={venue.id} value={venue.id}>{venue.name}</option>
            ))}
          </select>
        </div>

        {/* Race Selector */}
        <div className="flex items-center gap-2">
          <label htmlFor="race-select" className="font-medium">Race:</label>
          <select
            id="race-select"
            value={currentRace} // Use internal state
            onChange={handleRaceChange}
            disabled={loadingRaces || races.length === 0 || !currentVenue}
            className="bg-gray-700 border border-gray-600 rounded px-2 py-1 text-white focus:outline-none focus:border-blue-500 disabled:opacity-50"
          >
            <option value="">{loadingRaces ? 'Loading...' : 'Select Race...'}</option>
            {races.map((race) => (
              <option key={race.id} value={race.id}>{race.name}</option>
            ))}
          </select>
        </div>
      </div>
    </header>
  );
};

export default Header; 