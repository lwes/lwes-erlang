Light Weight Event System (LWES)
================================
For more information about lwes, see http://www.lwes.org/, for more information
about using lwes from erlang read on.

Creating Events
-------------------------
There are 2 ways of creating events, the functional way

```erlang
Event0 = lwes_event:new ("MyEvent"),
Event1 = lwes_event:set_uint16 (Event0, "MyUint16", 25),
```

or via records like

```erlang
Event = #lwes_event {
          name = "MyEvent",
          attrs = [{uint16, "MyUint16", 25}]
        },
```

Emitting to a single channel
-------------------------
If you are using multicast, or only want to emit to a single channel you
can open it as follows

```erlang
{ok, Channel0} = lwes:open (emitter, {Ip, Port})
Channel1 = lwes:emit (Channel0, Event1).
```

Emit to several channels
-------------------------
If you aren't using multicast but would like to emit to several machines,
or groups of machines you can with slightly different config,

```erlang
% emit to 1 of a set in a round robin fashion
{ok, Channels0} = lwes:open (emitters, {1, [{Ip1,Port1},...{IpN,PortN}]})
Channels1 = lwes:emit (Channels0, Event1)
Channels2 = lwes:emit (Channels1, Event2)
...
lwes:close (ChannelsN)

% emit to 2 of a set in an m of n fashion (ie, emit to first 2 in list,
% then 2nd and 3rd, then 3rd and 4th, etc., wraps at end of list)
{ok, Channels0} = lwes:open (emitters, {2, [{Ip1,Port1},...{IpN,PortN}]})
```

Listening via callback
-------------------------
```erlang
{ok, Channel} = lwes:open (listener, {Ip, Port})
lwes:listen (Channel, Fun, Type, Accum).
```
Fun is called for each event

Type is one of

<table>
  <tr>
    <th>raw</th><td>callback is given raw udp structure, use lwes_event:from_udp to turn into event</td>
  </tr>
  <tr>
    <th>list</th><td>callback is given an #lwes_event record where the name is a binary, and the attributes is a proplist where keys are binaries, and values are either integers (for lwes int types), binaries (for lwes strings), true|false atoms (for lwes booleans), or 4-tuples (for lwes ip addresses)</td>
  </tr>
  <tr>
    <th>tagged</th><td>callback is given an #lwes_event record where the name is a binary, and the attributes are 3-tuples with the first element the type of data, the second the key as a binary and the third the values as in the list format</td>
  </tr>
  <tr>
    <th>dict</th><td>callback is given an #lwes_event record where the name is a binary, and the attributes are a dictionary with a binary key and value according to the type</td>
  </tr>
  <tr>
    <th>json</th><td>this returns a proplist instead of an #lwes_event record.  The valuse are mostly the same as list, but ip addresses are strings (as binary).  This should means you can pass the returned value to mochijson2:encode (or other json encoders), and have the event as a json document</td>
  </tr>
</table>

Closing channel
-------------------------
```erlang
lwes:close (Channel)
```
