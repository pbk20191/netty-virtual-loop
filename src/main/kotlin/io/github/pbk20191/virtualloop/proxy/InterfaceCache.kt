package io.github.pbk20191.virtualloop.proxy

import java.lang.reflect.Modifier

internal object InterfaceCache : ClassValue<Array<Class<*>>>() {

    override fun computeValue(type: Class<*>): Array<Class<*>> {
        return generateSequence(type) { it.superclass }
            .flatMap {
                if (it.isInterface) {
                    sequenceOf(it) + sequenceOf(*it.interfaces)
                } else {
                    sequenceOf(*it.interfaces)
                }
            }
            .distinct()
            .filter { !it.isHidden && !it.isSealed && Modifier.isPublic(it.modifiers) }
            .toList()
            .toTypedArray()
    }

}