import { useState, useEffect } from "react";
import { getForecast } from "../api/financialApi";

export const useForecast = (ticker) => {
    const [forecast, setForecast] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (!ticker) {
            setForecast([]);
            setLoading(false);
            return;
        }
        setLoading(true);
        getForecast(ticker)
            .then(data => setForecast(data))
            .catch(err => setError(err))
            .finally(() => setLoading(false));
    }, [ticker]);

    return { forecast, loading, error };
};