import socket, asyncio


async def serveClient(reader, writer):
    t = (await reader.read(1024)).decode()
    print("Client " + str(writer.get_extra_info("peername")) + " sent " + str(t))
    writer.close()
    return

async def runServer():
    server = await asyncio.start_server(serveClient, socket.gethostname(), 1234)
    async with server:
        await server.serve_forever()

asyncio.run(runServer())
