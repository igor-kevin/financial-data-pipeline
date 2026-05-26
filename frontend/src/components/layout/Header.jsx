const Header = () => {
    const dataLocal = new Date().toLocaleDateString('pt-BR')
    return (
        <div className="flex justify-between items-center px-6 py-4 bg-gray-900 border-b border-gray-800"> 
            <span className='text-xl font-bold'>Financial Pipeline</span>
            <span className='text-blue-400 text-sm'>{dataLocal}</span>
        </div>
    )
}

export default Header