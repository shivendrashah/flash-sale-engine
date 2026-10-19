// =====================================================================================================
// Who you are shopping as. The backend has no login yet: it trusts the X-User-Id header
// The page makes up a name like "guest-4821", keeps it in localStorage, and lets you change it, so you
// can act as two buyers in two browser tabs (one tab can be "alice", the other "bob").
// A Context: every component below <BuyerProvider> can read the buyer with useBuyer(), without passing it
// down by hand.
// =====================================================================================================
import { createContext, useContext, useState, type ReactNode } from 'react';

const STORAGE_KEY = 'flashsale.buyer';

interface BuyerContextValue {
  buyer: string;
  setBuyer: (name: string) => void;
}

const BuyerContext = createContext<BuyerContextValue | null>(null);

function initialBuyer(): string {
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved) return saved;
  } catch {
    // private window or blocked storage: a new name each visit
  }
  return `guest-${Math.floor(1000 + Math.random() * 9000)}`;
}

export function BuyerProvider({ children }: { children: ReactNode }) {
  const [buyer, setBuyerState] = useState(initialBuyer);
  const setBuyer = (name: string) => {
    // the backend accepts up to 64 characters (HoldController's @Size)
    const clean = name.trim().slice(0, 64);
    if (!clean) return;
    setBuyerState(clean);
    try {
      localStorage.setItem(STORAGE_KEY, clean);
    } catch {
      // not saved; fine for this visit
    }
  };
  return <BuyerContext.Provider value={{ buyer, setBuyer }}>{children}</BuyerContext.Provider>;
}

export function useBuyer(): BuyerContextValue {
  const value = useContext(BuyerContext);
  if (!value) throw new Error('useBuyer must be used inside <BuyerProvider>');
  return value;
}
