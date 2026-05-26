import { useState, useEffect } from "react";
import { getComparativo } from "../api/financialApi";

export const useComparativo = (ticker) => {
    const [comparativo, setComparativo] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (!ticker) {
            setComparativo([]);
            setLoading(false);
            return;
        }
        setLoading(true);
        getComparativo(ticker)
            .then(data => setComparativo(data))
            .catch(err => setError(err))
            .finally(() => setLoading(false));
    }, [ticker]);

    return { comparativo, loading, error };
};