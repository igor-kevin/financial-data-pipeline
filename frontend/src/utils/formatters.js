export const formatPercent = (value) => {
    if (value === null || value === undefined) return '—'
    return `${(value * 100).toFixed(2)}%`
}

export const formatCdiPercent = (value) => {
    if (value === null || value === undefined) return '—'
    return `${value.toFixed(2)}% do CDI`
}

export const formatCurrency = (value, ticker) => {
    if (value === null || value === undefined) return '—'
    if (ticker === '^BVSP') return value.toLocaleString('pt-BR', { maximumFractionDigits: 0 }) + ' pts'
    return value.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' })
}