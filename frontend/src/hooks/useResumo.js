import { useState, useEffect } from "react";
import { getResumo } from "../api/financialApi";


export const useResumo = () => {
    const [resumo, setResumo] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        getResumo()
            .then(data => setResumo(data))
            .catch(err => setError(err))
            .finally(() => setLoading(false));
    }, []);

    return { resumo, loading, error };
};