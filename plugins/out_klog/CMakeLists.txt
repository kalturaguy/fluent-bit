set(src
  klog.c)

set(WITH_ZLIB On)

FLB_PLUGIN(out_klog "${src}" "")

if(WITH_ZLIB)
  find_package(ZLIB REQUIRED)
  target_link_libraries(flb-plugin-out_klog PUBLIC ZLIB::ZLIB)
endif()
