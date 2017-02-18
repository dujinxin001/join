import shipane_sdk

if __name__ == '__main__':
    _executor = shipane_sdk.JoinQuantExecutor(
        host='106.15.37.132',
        port=11788,
        key='',
        client=''
    )
    order=[]
    _executor.execute(order)
    print('1122')