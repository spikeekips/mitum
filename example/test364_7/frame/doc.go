/*
frame is the basic unit to transfer data thru network.

frame structure:: []bytes{frame type, chunk, [...], eof}
frame type:
  - stream frame: stream data
  - error frame: empty error is end of frame in frames

basic chunk: []bytes{length, [bytes]}
eof chunk: []bytes{0 length}
*/

package frame
