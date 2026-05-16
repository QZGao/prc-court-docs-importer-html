/* global mw, $ */
// <nowiki>
( function () {
	'use strict';

	function shouldEnableScript() {
		return [ 'edit', 'submit' ].indexOf( mw.config.get( 'wgAction' ) ) !== -1;
	}

	function addRedactButton( $textarea ) {
		if ( $textarea.data( 'prcRedactButtonAdded' ) ) {
			return;
		}

		$textarea.data( 'prcRedactButtonAdded', true );
		$textarea.wikiEditor( 'addToToolbar', {
			section: 'main',
			group: 'insert',
			tools: {
				redact: {
					label: 'Redact',
					type: 'button',
					icon: '//upload.wikimedia.org/wikipedia/commons/thumb/c/c5/OOjs_UI_icon_editUndo-ltr.svg/40px-OOjs_UI_icon_editUndo-ltr.svg.png',
					action: {
						type: 'callback',
						execute: function ( context ) {
							var selectedText = context.$textarea.textSelection( 'getSelection' );
							var replacement = '{{PRC-redact|' + Array.from( selectedText ).length + '|os=yes}}';

							context.$textarea.textSelection( 'replaceSelection', replacement );
						}
					}
				}
			}
		} );
	}

	if ( !shouldEnableScript() ) {
		return;
	}

	mw.loader.using( [ 'ext.wikiEditor', 'jquery.textSelection' ] ).then( function () {
		mw.hook( 'wikiEditor.toolbarReady' ).add( addRedactButton );
	} );
}() );
// </nowiki>
