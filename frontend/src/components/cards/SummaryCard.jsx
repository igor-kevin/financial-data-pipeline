import { formatPercent, formatCdiPercent, formatCurrency } from '../../utils/formatters'

const SummaryCard = ({ data }) => {
    const styles = {
        label: 'text-gray-400',
        card: 'bg-gray-900 rounded-lg p-4 flex flex-col gap-2',
        titulo: 'text-white font-bold text-lg',
        row: 'flex justify-between text-sm',
        corRetorno: data.retorno_365d >= 0 ? 'text-green-400': 'text-red-400',
        corCdi: data.pct_do_cdi_365d >= 100 ? 'text-green-400': 'text-red-400'
    }

    return (
        <div className={styles.card}>
            <h3 className={styles.titulo}>{data.ticker}</h3>
            <div className={styles.row}>
                <span className={styles.label}>Retorno 365d</span>
                <span className={styles.corRetorno}>
                    {formatPercent(data.retorno_365d)}
                </span>
            </div>
            <div className={styles.row}>
                <span className={styles.label}>vs CDI</span>
                <span className={styles.corCdi}>
                    {formatCdiPercent(data.pct_do_cdi_365d)}
                </span>
            </div>
            <div className={styles.row}>
                <span className={styles.label}>Drawdown</span>
                <span className="text-red-400">
                    {formatPercent(data.drawdown)}
                </span>
            </div>
        </div>
    )
}

export default SummaryCard;