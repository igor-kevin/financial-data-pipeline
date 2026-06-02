import { useState, useEffect } from "react";
import { getMetricas } from "../api/financialApi";

export const useMetricas = (ticker) => {
    const [metricas, setMetricas] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (!ticker) {
            setMetricas([]);
            setLoading(false);
            return;
        }
        setLoading(true);
        getMetricas(ticker)
            .then(data => setMetricas(data))
            .catch(err => setError(err))
            .finally(() => setLoading(false));
    }, [ticker]);

    return { metricas, loading, error };
};