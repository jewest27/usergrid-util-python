import traceback

try:
    from usergrid import UsergridQueryIterator

    q = UsergridQueryIterator('')

    print 'Check OK'

except Exception, e:
    print traceback.format_exc(e)
    print 'Check Failed'