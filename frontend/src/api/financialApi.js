import api from './axios'


export const getTickers = async() => {
    const response = await api.get('/ticker')
    return response.data
}

export const getResumo = async () => {
    const response = await api.get(`/resumo`)
    return response.data
}

export const getComparativo = async (ticker) => {
    const response = await api.get(`/comparativo/${ticker}`)
    return response.data
}

export const getMetricas = async (ticker) => {
    const response = await api.get(`/metricas/${ticker}`)
    return response.data
}