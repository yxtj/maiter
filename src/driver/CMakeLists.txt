set(TOOLS
	tools/ReplyHandler.h
	tools/ReplyHandler.cc
	tools/SyncUnit.h
	tools/SyncUnit.cc
)

add_library(driver
	Dispatcher.hpp
	MsgDriver.h
	MsgDriver.cc
	${TOOLS}
)