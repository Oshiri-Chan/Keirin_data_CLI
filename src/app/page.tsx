"use client"; // Make this a client component

import React, { useState, useCallback } from 'react';
import Header from '@/components/viewer/Header'; // Updated import path
import RaceCardDisplay from '@/components/viewer/RaceCardDisplay'; // Import the new component
// import PlayerHistory from '@/components/viewer/PlayerHistory'; // Import later

export default function Home() {
  return (
    <div>
      <h1>テストページ</h1>
      <p>このメッセージが表示されれば、page.tsxは更新されています。</p>
    </div>
  );
} 