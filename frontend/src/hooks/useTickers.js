import {useState, useEffect} from 'react'
import { getTickers } from '../api/financialApi'

export const useTickers = () =>{ 
    const [tickers, setTickers] = useState([])
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState(null)

    useEffect(() => {
        getTickers()
            .then(data => {
                console.log('tickers:', data)  // adicione isso
                setTickers(data)
            })
            .catch(err => {
                console.error('erro:', err)    // e isso
                setError(err)
            })
            .finally(() => setLoading(false))
    }, [])

    return {tickers, loading, error}

}

