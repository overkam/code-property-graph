import { Navigate, Route, Routes } from 'react-router-dom';
import { ExplorerPage } from '@/pages/explorer';

export default function App() {
  return (
    <Routes>
      <Route path="/explore" element={<ExplorerPage />} />
      <Route path="/" element={<Navigate to="/explore" replace />} />
    </Routes>
  );
}
