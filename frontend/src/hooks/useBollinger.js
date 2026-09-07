import { useState, useEffect } from "react";
import { getBollinger } from "../api/financialApi";

export const useBollinger = (ticker) => {
    const [bollinger, setBollinger] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (!ticker) {
            setBollinger([]);
            setLoading(false);
            return;
        }
        setLoading(true);
        getBollinger(ticker)
            .then(data => setBollinger(data))
            .catch(err => setError(err))
            .finally(() => setLoading(false));
    }, [ticker]);

    return { bollinger, loading, error };
};